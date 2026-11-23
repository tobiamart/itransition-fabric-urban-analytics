section Section1;
shared OpenAQApiKey = "{{secret:OPENAQ_API_KEY}}" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations = let
    // Settings
    BoundingBox = "-74.26,40.49,-73.70,40.92",    // NYC: min lon, min lat, max lon, max lat

    // All pages of an OpenAQ v3 list endpoint (max 1000 rows per page). The key goes in the
    // X-API-Key header, and calls are spaced 1 s apart to stay under the 60 requests/min limit.
    GetAll = (path as text, query as record) as list =>
        let
            Page = (n as number) as list => Function.InvokeAfter(
                () => Json.Document(Web.Contents("https://api.openaq.org/v3", [
                    RelativePath = path,
                    Query = query & [limit = "1000", page = Text.From(n)],
                    Headers = [#"X-API-Key" = OpenAQApiKey]
                ]))[results],
                #duration(0, 0, 0, 1)),
            Pages = List.Generate(
                () => [n = 1, rows = Page(1)],
                each [rows] <> null,
                each if List.Count([rows]) < 1000 then [n = [n], rows = null] else [n = [n] + 1, rows = Page([n] + 1)],
                each [rows])
        in
            List.Combine(Pages),

    Locations = GetAll("locations", [bbox = BoundingBox]),
    // One row per sensor: a location has a sensor per measured parameter
    Rows = List.Combine(List.Transform(Locations, (loc) => List.Transform(loc[sensors], (s) => [
        location_id = loc[id],
        location_name = loc[name],
        locality = loc[locality],
        provider = loc[provider][name],
        is_monitor = loc[isMonitor],
        latitude = loc[coordinates][latitude],
        longitude = loc[coordinates][longitude],
        datetime_first_utc = try loc[datetimeFirst][utc] otherwise null,
        datetime_last_utc = try loc[datetimeLast][utc] otherwise null,
        sensor_id = s[id],
        parameter_id = s[parameter][id],
        parameter = s[parameter][name],
        units = s[parameter][units]
    ]))),
    Table = Table.FromRecords(Rows, type table [
        location_id = Int64.Type, location_name = text, locality = nullable text, provider = text,
        is_monitor = logical, latitude = number, longitude = number,
        datetime_first_utc = nullable text, datetime_last_utc = nullable text,
        sensor_id = Int64.Type, parameter_id = Int64.Type, parameter = text, units = text
    ]),
    IngestedAt = DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
    WithIngestedAt = Table.AddColumn(Table, "_ingested_at", each IngestedAt, type datetime),
    WithSource = Table.AddColumn(WithIngestedAt, "_source", each "openaq:v3/locations", type text)
in
    WithSource;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_daily_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_daily = let
    // Settings: pollutants and period (same months as the taxi data)
    BoundingBox = "-74.26,40.49,-73.70,40.92",
    Parameters = {"pm25", "no2", "o3"},
    DateFrom = "2025-10-01",
    DateTo = "2026-10-01",

    GetAll = (path as text, query as record) as list =>
        let
            Page = (n as number) as list => Function.InvokeAfter(
                () => Json.Document(Web.Contents("https://api.openaq.org/v3", [
                    RelativePath = path,
                    Query = query & [limit = "1000", page = Text.From(n)],
                    Headers = [#"X-API-Key" = OpenAQApiKey]
                ]))[results],
                #duration(0, 0, 0, 1)),
            Pages = List.Generate(
                () => [n = 1, rows = Page(1)],
                each [rows] <> null,
                each if List.Count([rows]) < 1000 then [n = [n], rows = null] else [n = [n] + 1, rows = Page([n] + 1)],
                each [rows])
        in
            List.Combine(Pages),

    // Sensors in the bounding box that measure a wanted pollutant and were active in the period
    Locations = GetAll("locations", [bbox = BoundingBox]),
    Active = List.Select(Locations, each (try _[datetimeLast][utc] otherwise "") >= DateFrom),
    Sensors = List.Combine(List.Transform(Active, (loc) => List.Transform(
        List.Select(loc[sensors], each List.Contains(Parameters, _[parameter][name])),
        (s) => [location_id = loc[id], sensor_id = s[id]]))),

    // Daily summaries (local calendar days) for every sensor
    Days = List.Combine(List.Transform(Sensors, (sensor) => List.Transform(
        GetAll("sensors/" & Text.From(sensor[sensor_id]) & "/days", [date_from = DateFrom, date_to = DateTo]),
        (d) => [
            location_id = sensor[location_id],
            sensor_id = sensor[sensor_id],
            parameter = d[parameter][name],
            units = d[parameter][units],
            period_from_local = d[period][datetimeFrom][local],
            period_from_utc = d[period][datetimeFrom][utc],
            value_avg = d[value],
            value_min = try d[summary][min] otherwise null,
            value_max = try d[summary][max] otherwise null,
            value_median = try d[summary][median] otherwise null,
            observed_count = try d[coverage][observedCount] otherwise null,
            expected_count = try d[coverage][expectedCount] otherwise null,
            percent_complete = try d[coverage][percentComplete] otherwise null
        ]))),
    Table = Table.FromRecords(Days, type table [
        location_id = Int64.Type, sensor_id = Int64.Type, parameter = text, units = text,
        period_from_local = text, period_from_utc = text,
        value_avg = nullable number, value_min = nullable number, value_max = nullable number,
        value_median = nullable number, observed_count = nullable Int64.Type,
        expected_count = nullable Int64.Type, percent_complete = nullable number
    ]),
    IngestedAt = DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
    WithIngestedAt = Table.AddColumn(Table, "_ingested_at", each IngestedAt, type datetime),
    WithSource = Table.AddColumn(WithIngestedAt, "_source", each "openaq:v3/sensors/{id}/days", type text)
in
    WithSource;
shared bronze_openaq_locations_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "{{workspace}}"]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = "{{item:lh_bronze.Lakehouse}}"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_locations", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
shared bronze_openaq_daily_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "{{workspace}}"]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = "{{item:lh_bronze.Lakehouse}}"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_daily", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
