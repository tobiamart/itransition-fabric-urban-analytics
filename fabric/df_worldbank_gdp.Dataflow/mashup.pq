section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_worldbank_gdp_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_worldbank_gdp = let
    // Settings: add a country code (ISO3) to load more countries.
    Countries = {"USA"},
    Indicator = "NY.GDP.MKTP.CD",

    // The API returns [paging info, records]; all pages are read, so more countries still work.
    GetPage = (page as number) as list =>
        Json.Document(Web.Contents(
            "https://api.worldbank.org/v2",
            [
                RelativePath = "country/" & Text.Combine(Countries, ";") & "/indicator/" & Indicator,
                Query = [format = "json", per_page = "1000", page = Text.From(page)]
            ]
        )),
    FirstPage = GetPage(1),
    PageCount = FirstPage{0}[pages],
    Records = List.Combine({FirstPage{1}} & List.Transform({2..PageCount}, each GetPage(_){1})),
    Rows = Table.FromRecords(List.Transform(Records, each [
        country_code = [countryiso3code],
        country_name = [country][value],
        indicator_id = [indicator][id],
        indicator_name = [indicator][value],
        year = [date],
        value = [value],
        unit = [unit],
        obs_status = [obs_status]
    ])),
    Typed = Table.TransformColumnTypes(Rows, {
        {"country_code", type text}, {"country_name", type text}, {"indicator_id", type text},
        {"indicator_name", type text}, {"year", type text}, {"value", type number},
        {"unit", type text}, {"obs_status", type text}
    }),
    IngestedAt = DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
    WithIngestedAt = Table.AddColumn(Typed, "_ingested_at", each IngestedAt, type datetime),
    WithSource = Table.AddColumn(WithIngestedAt, "_source", each "worldbank:" & Indicator, type text)
in
    WithSource;
shared bronze_worldbank_gdp_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "{{workspace}}"]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = "{{item:lh_bronze.Lakehouse}}"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_worldbank_gdp", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
