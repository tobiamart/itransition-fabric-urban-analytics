section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_ecb_fx_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_ecb_fx = let
    // Daily USD/EUR reference rate published by the ECB, full history since 1999.
    // Bronze keeps every column exactly as published (as text); typing happens in Silver.
    Source = Web.Contents(
        "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
        [Query = [format = "csvdata"]]
    ),
    Csv = Csv.Document(Source, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Csv, [PromoteAllScalars = true]),
    AsText = Table.TransformColumnTypes(Promoted, List.Transform(Table.ColumnNames(Promoted), each {_, type text})),
    IngestedAt = DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
    WithIngestedAt = Table.AddColumn(AsText, "_ingested_at", each IngestedAt, type datetime),
    WithSource = Table.AddColumn(WithIngestedAt, "_source", each "ecb:EXR/D.USD.EUR.SP00.A", type text)
in
    WithSource;
shared bronze_ecb_fx_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "{{workspace}}"]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = "{{item:lh_bronze.Lakehouse}}"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_ecb_fx", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
