import dlt
from pyspark.sql import functions as F

# ---- Settings: pipeline ki Configuration (Step 6) se aate hain, code mein koi secret nahi ----
EH_NAMESPACE = spark.conf.get("eh.namespace")   # ehn-retail-nde
EH_HUB       = spark.conf.get("eh.hub")         # pos-sales
EH_CONN      = spark.conf.get("eh.conn")        # Key Vault se (eh-pos-listen)
LANDING      = spark.conf.get("landing.path")   # abfss://landing@retailstoragelakende.dfs.core.windows.net

JAAS = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" password="' + EH_CONN + '";')


# ---- 1. POS events: Event Hubs (Kafka endpoint) -> bronze.pos_sales ----
@dlt.table(name="bronze.pos_sales", comment="Raw POS events from Event Hubs, unparsed")
def pos_sales():
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", EH_NAMESPACE + ".servicebus.windows.net:9093")
            .option("subscribe", EH_HUB)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", JAAS)
            .option("kafka.group.id", "databricks")
            .option("kafka.request.timeout.ms", "60000")
            .option("kafka.session.timeout.ms", "30000")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("raw_json"),
                    F.col("partition"),
                    F.col("offset"),
                    F.col("timestamp").alias("eh_enqueued_ts"),
                    F.current_timestamp().alias("_ingested_at")))


# ---- Helper: Auto Loader, files ke liye ek hi pattern ----
def autoloader(folder, fmt):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("rescuedDataColumn", "_rescued_data"))
    if fmt == "csv":
        reader = reader.option("header", "true")
    return (reader.load(LANDING + "/" + folder + "/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
            .withColumn("_ingested_at", F.current_timestamp()))


# ---- 2. Supplier shipments: landing/shipments/SUP_1..SUP_5 ----
@dlt.table(name="bronze.shipments", comment="Raw supplier shipment lines, as delivered")
def shipments():
    return autoloader("shipments", "json")


# ---- 3. Weather: landing/weather ----
@dlt.table(name="bronze.weather", comment="Raw daily weather files")
def weather():
    return autoloader("weather", "json")


# ---- 4. Calendar: landing/reference/calendar.csv ----
@dlt.table(name="bronze.calendar", comment="M5 calendar (holidays, events, SNAP)")
def calendar():
    return autoloader("reference", "csv")