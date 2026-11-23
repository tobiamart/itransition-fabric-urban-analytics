"""Deploy and export Fabric workspace items through the Fabric REST API.

The university tenant does not allow Fabric Git integration with GitHub, so the items are
kept in `fabric/` in the same folder format that Git integration uses (`<name>.<Type>/` with
the item's definition files) and synchronised with this script:

    python tools/fabric_sync.py login                     # once, opens the browser
    python tools/fabric_sync.py items                     # list workspace items
    python tools/fabric_sync.py connect                   # create connections from fabric/connections
    python tools/fabric_sync.py deploy fabric/pl_copy_taxi_file.DataPipeline
    python tools/fabric_sync.py export pl_ingest_taxi.DataPipeline
    python tools/fabric_sync.py upload config/sources.json lh_bronze.Lakehouse Files/config/sources.json
    python tools/fabric_sync.py run pl_ingest_taxi --wait
    python tools/fabric_sync.py ls lh_bronze.Lakehouse Files/taxi

Item and workspace ids differ between workspaces, so definitions in Git use placeholders that
are resolved on deploy and restored on export:

    {{workspace}}             id of the target workspace (config/fabric.json)
    {{item:NAME.Type}}        id of an item in that workspace, e.g. {{item:lh_bronze.Lakehouse}}
    {{connection:NAME}}       id of a Fabric connection, by display name
    {{secret:NAME}}           value of NAME from .env (never committed, see local_secrets.py)
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
from pathlib import Path

import requests
from azure.identity import (
    AuthenticationRecord,
    AuthenticationRequiredError,
    DeviceCodeCredential,
    InteractiveBrowserCredential,
    TokenCachePersistenceOptions,
)

import local_secrets

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "config" / "fabric.json").read_text(encoding="utf-8"))

API = "https://api.fabric.microsoft.com/v1"
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"

# Sign-in state lives in the user's profile, never in the repository.
AUTH_RECORD = Path.home() / ".fabric_sync" / "auth_record.json"
TOKEN_CACHE = TokenCachePersistenceOptions(name="fabric_sync")

PLACEHOLDER = re.compile(r"\{\{(workspace|item|connection|secret)(?::([^}]+))?\}\}")
FINISHED = {"Completed", "Failed", "Cancelled", "Deduped"}
JOB_TYPES = {"DataPipeline": "Pipeline", "Dataflow": "Refresh", "Notebook": "RunNotebook"}


def b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def fail(message: str) -> None:
    sys.exit(f"error: {local_secrets.redact(message)}")


class Fabric:
    """Thin wrapper around the Fabric REST API for one workspace."""

    def __init__(self) -> None:
        if not AUTH_RECORD.exists():
            fail("not signed in, run: python tools/fabric_sync.py login")
        record = AuthenticationRecord.deserialize(AUTH_RECORD.read_text(encoding="utf-8"))
        self.credential = InteractiveBrowserCredential(
            tenant_id=record.tenant_id,
            authentication_record=record,
            cache_persistence_options=TOKEN_CACHE,
            disable_automatic_authentication=True,  # never pop up a browser unexpectedly
        )
        self.session = requests.Session()
        self._tokens: dict = {}
        self._items: list | None = None
        self._connections: list | None = None
        self.workspace_id = self._find_workspace(CONFIG["workspace"])

    # --- HTTP -----------------------------------------------------------------------------

    def token(self, scope: str) -> str:
        cached = self._tokens.get(scope)
        if cached is None or cached.expires_on - time.time() < 300:
            try:
                cached = self.credential.get_token(scope)
            except AuthenticationRequiredError:
                fail("sign-in expired, run: python tools/fabric_sync.py login")
            self._tokens[scope] = cached
        return cached.token

    def request(self, method: str, path: str, body=None, scope: str = FABRIC_SCOPE, **kwargs):
        url = path if path.startswith("https://") else f"{API}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.token(scope)}", **kwargs.pop("headers", {})}
        for _ in range(5):
            response = self.session.request(method, url, json=body, headers=headers, timeout=120, **kwargs)
            if response.status_code != 429:
                break
            time.sleep(int(response.headers.get("Retry-After", 10)))
        if response.status_code >= 400:
            fail(f"{method} {url} -> {response.status_code}\n{response.text}")
        return response

    def call(self, method: str, path: str, body=None) -> dict:
        """Call the API and wait for long-running operations to finish."""
        response = self.request(method, path, body)
        operation = response.headers.get("x-ms-operation-id")
        if response.status_code == 202 and operation:
            return self._wait_for_operation(operation, int(response.headers.get("Retry-After", 2)))
        return response.json() if response.content else {}

    def _wait_for_operation(self, operation: str, delay: int) -> dict:
        while True:
            time.sleep(delay)
            state = self.request("GET", f"operations/{operation}").json()
            if state["status"] == "Succeeded":
                result = self.session.get(
                    f"{API}/operations/{operation}/result",
                    headers={"Authorization": f"Bearer {self.token(FABRIC_SCOPE)}"},
                    timeout=120,
                )
                return result.json() if result.status_code == 200 and result.content else {}
            if state["status"] in ("Failed", "Undefined"):
                fail(f"operation {operation} failed:\n{json.dumps(state.get('error'), indent=2)}")

    def list(self, path: str) -> list:
        values, url = [], path
        while url:
            page = self.request("GET", url).json()
            values += page.get("value", [])
            url = page.get("continuationUri")
        return values

    # --- lookups --------------------------------------------------------------------------

    def _find_workspace(self, name: str) -> str:
        for workspace in self.list("workspaces"):
            if workspace["displayName"] == name:
                return workspace["id"]
        fail(f"workspace '{name}' not found")

    def items(self, refresh: bool = False) -> list:
        if self._items is None or refresh:
            self._items = self.list(f"workspaces/{self.workspace_id}/items")
        return self._items

    def find_item(self, ref: str) -> dict | None:
        """`ref` is NAME.Type, e.g. lh_bronze.Lakehouse (a lakehouse and its SQL endpoint share a name)."""
        name, _, item_type = ref.rpartition(".")
        for item in self.items():
            if item["displayName"] == name and item["type"] == item_type:
                return item
        return None

    def item_id(self, ref: str) -> str:
        item = self.find_item(ref)
        if item is None:
            fail(f"item '{ref}' not found in workspace '{CONFIG['workspace']}'")
        return item["id"]

    def connections(self) -> list:
        if self._connections is None:
            self._connections = self.list("connections")
        return self._connections

    def connection_id(self, name: str) -> str:
        # Connections created in the dataflow editor get the user's alias appended ("name alias"),
        # so a placeholder name also matches "name <anything>".
        for connection in self.connections():
            if connection.get("displayName", "").split(" ")[0] == name:
                return connection["id"]
        fail(f"connection '{name}' not found")

    # --- placeholders ---------------------------------------------------------------------

    def resolve(self, text: str) -> str:
        def replace(match: re.Match) -> str:
            kind, name = match.groups()
            if kind == "workspace":
                return self.workspace_id
            if kind == "item":
                return self.item_id(name)
            if kind == "secret":
                return local_secrets.get(name)
            return self.connection_id(name)

        return PLACEHOLDER.sub(replace, text)

    def unresolve(self, text: str) -> str:
        for name, value in local_secrets.searchable().items():
            text = text.replace(value, "{{secret:%s}}" % name)
        known = {self.workspace_id: "{{workspace}}"}
        for item in self.items():
            known[item["id"]] = "{{item:%s.%s}}" % (item["displayName"], item["type"])
        for connection in self.connections():
            if connection.get("displayName"):  # without the appended user alias, see connection_id
                known[connection["id"]] = "{{connection:%s}}" % connection["displayName"].split(" ")[0]
        for item_id, placeholder in known.items():
            text = re.sub(re.escape(item_id), placeholder, text, flags=re.IGNORECASE)
        return text


# --- commands -------------------------------------------------------------------------------


def cmd_login(args) -> None:
    if args.device_code:
        # Fallback when no browser can be opened: sign in at microsoft.com/devicelogin with a code.
        credential = DeviceCodeCredential(tenant_id=args.tenant, cache_persistence_options=TOKEN_CACHE,
                                          timeout=args.timeout)
    else:
        credential = InteractiveBrowserCredential(tenant_id=args.tenant, cache_persistence_options=TOKEN_CACHE,
                                                  timeout=args.timeout)
    record = credential.authenticate(scopes=[FABRIC_SCOPE])
    AUTH_RECORD.parent.mkdir(exist_ok=True)
    AUTH_RECORD.write_text(record.serialize(), encoding="utf-8")
    print("signed in")


def cmd_items(args) -> None:
    fabric = Fabric()
    for item in sorted(fabric.items(), key=lambda i: (i["type"], i["displayName"])):
        print(f"{item['type']:<22} {item['displayName']:<30} {item['id']}")


def read_parts(folder: Path, fabric: Fabric) -> list:
    parts = []
    for file in sorted(folder.rglob("*")):
        if file.is_file():
            content = fabric.resolve(file.read_text(encoding="utf-8"))
            parts.append(
                {"path": file.relative_to(folder).as_posix(), "payload": b64(content), "payloadType": "InlineBase64"}
            )
    return parts


def cmd_deploy(args) -> None:
    fabric = Fabric()
    for path in args.folders:
        folder = Path(path)
        if not folder.is_dir() or "." not in folder.name:
            fail(f"{path} is not an item folder like fabric/NAME.Type")
        ref = folder.name
        name, _, item_type = ref.rpartition(".")
        definition = {"parts": read_parts(folder, fabric)}
        existing = fabric.find_item(ref)
        if existing:
            fabric.call("POST", f"workspaces/{fabric.workspace_id}/items/{existing['id']}/updateDefinition",
                        {"definition": definition})
            print(f"updated  {ref}")
        else:
            fabric.call("POST", f"workspaces/{fabric.workspace_id}/items",
                        {"displayName": name, "type": item_type, "definition": definition})
            fabric.items(refresh=True)
            print(f"created  {ref}")


def cmd_export(args) -> None:
    fabric = Fabric()
    for ref in args.items:
        item_id = fabric.item_id(ref)
        result = fabric.call("POST", f"workspaces/{fabric.workspace_id}/items/{item_id}/getDefinition")
        folder = ROOT / "fabric" / ref
        for part in result["definition"]["parts"]:
            if part["path"] == ".platform":
                continue  # workspace metadata, recreated by Fabric on deploy
            content = base64.b64decode(part["payload"]).decode("utf-8")
            if part["path"].endswith(".json"):
                content = json.dumps(json.loads(content), indent=2, ensure_ascii=False) + "\n"
            content = fabric.unresolve(content)
            if local_secrets.leaks(content):
                fail(f"{ref}/{part['path']} still contains a secret value, nothing written")
            target = folder / part["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8", newline="\n")
            print(f"exported {target.relative_to(ROOT).as_posix()}")


def cmd_connect(args) -> None:
    """Create the connections described in fabric/connections/*.json that do not exist yet."""
    fabric = Fabric()
    for file in sorted((ROOT / "fabric" / "connections").glob("*.json")):
        body = json.loads(file.read_text(encoding="utf-8"))
        name = body["displayName"]
        if any(c.get("displayName", "").split(" ")[0] == name for c in fabric.connections()):
            print(f"exists   connection {name}")
            continue
        fabric.call("POST", "connections", body)
        print(f"created  connection {name}")
    fabric._connections = None


def cmd_upload(args) -> None:
    fabric = Fabric()
    data = Path(args.source).read_bytes()
    url = f"{ONELAKE}/{fabric.workspace_id}/{fabric.item_id(args.item)}/{args.destination.lstrip('/')}"
    version = {"x-ms-version": "2023-11-03"}
    fabric.request("PUT", f"{url}?resource=file", scope=STORAGE_SCOPE, headers=version)
    fabric.request("PATCH", f"{url}?action=append&position=0", scope=STORAGE_SCOPE, headers=version, data=data)
    fabric.request("PATCH", f"{url}?action=flush&position={len(data)}", scope=STORAGE_SCOPE, headers=version)
    print(f"uploaded {args.source} -> {args.item}/{args.destination} ({len(data)} bytes)")


def cmd_ls(args) -> None:
    fabric = Fabric()
    directory = f"{fabric.item_id(args.item)}/{args.path.strip('/')}"
    response = fabric.request(
        "GET", f"{ONELAKE}/{fabric.workspace_id}?resource=filesystem&recursive=true&directory={directory}",
        scope=STORAGE_SCOPE, headers={"x-ms-version": "2023-11-03"},
    )
    for path in response.json().get("paths", []):
        if path.get("isDirectory") != "true":
            name = path["name"].split("/", 1)[1]
            print(f"{int(path.get('contentLength', 0)) / 1e6:>8.1f} MB  {path.get('lastModified', '')}  {name}")


def cmd_tables(args) -> None:
    """Delta tables of a lakehouse with their current row count, read from the Delta log."""
    fabric = Fabric()
    item = fabric.item_id(args.item)
    version = {"x-ms-version": "2023-11-03"}

    def children(path: str) -> list:
        response = fabric.request(
            "GET", f"{ONELAKE}/{fabric.workspace_id}?resource=filesystem&recursive=false&directory={item}/{path}",
            scope=STORAGE_SCOPE, headers=version)
        return response.json().get("paths", [])

    def read(name: str) -> str:
        return fabric.request("GET", f"{ONELAKE}/{fabric.workspace_id}/{name}", scope=STORAGE_SCOPE,
                              headers=version).text

    for schema in (p["name"].rsplit("/", 1)[1] for p in children("Tables") if p.get("isDirectory") == "true"):
        for table in (p["name"].rsplit("/", 1)[1] for p in children(f"Tables/{schema}")
                      if p.get("isDirectory") == "true"):
            log = sorted(p["name"] for p in children(f"Tables/{schema}/{table}/_delta_log")
                         if p["name"].endswith(".json"))
            files: dict = {}
            for name in log:
                for line in read(name).splitlines():
                    action = json.loads(line)
                    if "add" in action:
                        files[action["add"]["path"]] = json.loads(action["add"].get("stats") or "{}").get("numRecords")
                    elif "remove" in action:
                        files.pop(action["remove"]["path"], None)
            counts = list(files.values())
            rows = sum(counts) if None not in counts else "?"
            print(f"{schema}.{table:<32} {rows:>12,} rows  {len(files)} files  version {len(log) - 1}")


def cmd_run(args) -> None:
    fabric = Fabric()
    ref = args.item if "." in args.item else f"{args.item}.DataPipeline"
    job_type = JOB_TYPES.get(ref.rpartition(".")[2])
    if job_type is None:
        fail(f"cannot run {ref}, supported types: {', '.join(JOB_TYPES)}")
    parameters = dict(p.split("=", 1) for p in args.param)
    body = {"executionData": {"parameters": parameters}} if parameters else None
    response = fabric.request(
        "POST", f"workspaces/{fabric.workspace_id}/items/{fabric.item_id(ref)}/jobs/instances?jobType={job_type}", body)
    location = response.headers["Location"]
    print(f"started  {ref}: {location.rsplit('/', 1)[-1]}")
    if not args.wait:
        return
    status, started = None, time.time()
    while status not in FINISHED:
        time.sleep(15)
        job = fabric.request("GET", location).json()
        if job["status"] != status:
            status = job["status"]
            print(f"{int(time.time() - started):>5}s  {status}")
    if status != "Completed":
        fail(json.dumps(job.get("failureReason"), indent=2))


def cmd_jobs(args) -> None:
    """Latest run of each item; with --wait, poll until all of them have finished."""
    fabric = Fabric()
    while True:
        latest = {}
        for ref in args.items:
            runs = fabric.list(f"workspaces/{fabric.workspace_id}/items/{fabric.item_id(ref)}/jobs/instances")
            latest[ref] = max(runs, key=lambda r: r.get("startTimeUtc") or "") if runs else None
        if not args.wait or all(r is None or r["status"] in FINISHED for r in latest.values()):
            break
        time.sleep(20)
    for ref, run in latest.items():
        if run is None:
            print(f"{ref:<32} never run")
            continue
        error = (run.get("failureReason") or {}).get("message", "")
        print(f"{ref:<32} {run['status']:<11} {run.get('startTimeUtc', '')[:19]}  {run.get('endTimeUtc') or ''}"[:100])
        if error:
            print(f"    {local_secrets.redact(error)}")


def cmd_api(args) -> None:
    if args.path.rstrip("/").endswith("getDefinition"):
        fail("use the export command: it removes secrets, a raw getDefinition would print them")
    fabric = Fabric()
    body = json.loads(Path(args.body).read_text(encoding="utf-8")) if args.body else None
    print(json.dumps(fabric.call(args.method.upper(), fabric.resolve(args.path), body), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(required=True)

    login = commands.add_parser("login", help="sign in with the browser (once)")
    login.add_argument("--tenant", default="organizations", help="Entra tenant id or domain")
    login.add_argument("--device-code", action="store_true", help="sign in with a code instead of a browser window")
    login.add_argument("--timeout", type=int, default=900, help="seconds to wait for the sign-in")
    login.set_defaults(func=cmd_login)

    commands.add_parser("items", help="list workspace items").set_defaults(func=cmd_items)

    commands.add_parser("connect", help="create missing connections from fabric/connections/*.json").set_defaults(
        func=cmd_connect)

    deploy = commands.add_parser("deploy", help="create or update items from fabric/NAME.Type folders")
    deploy.add_argument("folders", nargs="+")
    deploy.set_defaults(func=cmd_deploy)

    export = commands.add_parser("export", help="write item definitions to fabric/NAME.Type")
    export.add_argument("items", nargs="+", help="NAME.Type, e.g. pl_ingest_taxi.DataPipeline")
    export.set_defaults(func=cmd_export)

    upload = commands.add_parser("upload", help="upload a local file to a lakehouse")
    upload.add_argument("source")
    upload.add_argument("item", help="NAME.Type, e.g. lh_bronze.Lakehouse")
    upload.add_argument("destination", help="path inside the item, e.g. Files/config/sources.json")
    upload.set_defaults(func=cmd_upload)

    ls = commands.add_parser("ls", help="list files in a lakehouse folder")
    ls.add_argument("item", help="NAME.Type, e.g. lh_bronze.Lakehouse")
    ls.add_argument("path", help="folder inside the item, e.g. Files/taxi")
    ls.set_defaults(func=cmd_ls)

    tables = commands.add_parser("tables", help="Delta tables of a lakehouse with row counts")
    tables.add_argument("item", help="NAME.Type, e.g. lh_bronze.Lakehouse")
    tables.set_defaults(func=cmd_tables)

    run = commands.add_parser("run", help="run a pipeline, dataflow or notebook")
    run.add_argument("item", help="NAME (a pipeline) or NAME.Type, e.g. df_ecb_fx.Dataflow")
    run.add_argument("--param", action="append", default=[], help="name=value, repeatable")
    run.add_argument("--wait", action="store_true", help="wait and print status changes")
    run.set_defaults(func=cmd_run)

    jobs = commands.add_parser("jobs", help="latest run of pipelines / dataflows / notebooks")
    jobs.add_argument("items", nargs="+", help="NAME.Type, e.g. df_openaq.Dataflow")
    jobs.add_argument("--wait", action="store_true", help="wait until the runs have finished")
    jobs.set_defaults(func=cmd_jobs)

    api = commands.add_parser("api", help="raw API call, placeholders allowed in the path")
    api.add_argument("method")
    api.add_argument("path")
    api.add_argument("--body", help="JSON file with the request body")
    api.set_defaults(func=cmd_api)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
