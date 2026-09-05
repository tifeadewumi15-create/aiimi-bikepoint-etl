# Databricks notebook source
# MAGIC %md
# MAGIC Testing connection below

# COMMAND ----------

print("Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC To begin with, I want to actually see what data the TFL gives me. I will pull the data from the TFL link given.
# MAGIC - Requests.get calls the API back for me to get the raw JSON text
# MAGIC - .json is used to help me parse it into Python (essentially translates the text into something Python can understand)
# MAGIC - "data" is the list of all of the bike records pulled from the API

# COMMAND ----------

import requests

response = requests.get("https://api.tfl.gov.uk/BikePoint/")
data = response.json()

print(f"Number of bike points: {len(data)}")
print(data[0])

# COMMAND ----------

# MAGIC %md
# MAGIC What I found from the TFL BikePoint API:
# MAGIC
# MAGIC - The API returns a list of 800 bikepoints, each with some fields sitting flat, directly on the record like id and commonName
# MAGIC - The real detail that I'm looking for (which includes data such as bike counts and dock counts) don't sit as flat fields though. They are packed inside a list called "AdditionalProperties", where each item has its own little record with a key (the name of the attribute, like NbBikes), and a value (like '17')
# MAGIC - I am trying to think of this like a folder. The stuff sitting directly on the record (such as lat, lon, commonName, id) are written on the cover of the folder, so it's easy to read straight away. Then additionalProperties is like a stack of sticky notes stuffed inside the folder, one note per fact.
# MAGIC - I need to find a way to actually use this data properly. I will use the data flattening technique to open up those sticky notes and turn them into real columns. 
# MAGIC - For structuring this pipeline, I'm following the Medallion architecture (Bronze, Silver, Gold), which maps directly onto Extract, Transform, Load. Bronze will hold the raw data from the API exactly as it arrived, Silver is where I will clean and flatten it, and Gold will contain the final, shaped data ready to answer the Mayor's question.
# MAGIC
# MAGIC -  Next step: Now that I understand the shape of the data, I'm going to start the Extract stage of my ETL pipeline. I'll convert the raw data into a Spark Data Frame and save it as a Delta table called bronze_bikepoints. This is to keep an untouched, raw copy of what the API gave me, so if anything goes wrong later in my cleaning / transform steps, I can always come back to this original version rather than having to call the API again.
# MAGIC
# MAGIC  - Side note: I noticed that every value on the sticky notes (the 'AdditionalProperties' list) comes through as text,
# MAGIC  including the numbers so '17' instead of 17. I'll need to convert these to actual numbers later on, otherwise sorting and calculations won't work properly.

# COMMAND ----------

# EXTRACT step: I'm turning the raw data into a table I can actually work with

# TfL has left 'children' and 'childrenUrls' empty for BikePoint entries,
# Spark can't guess a type for a field that's always an empty list, so I'm dropping them
cleaned_data = [
    {key: value for key, value in record.items() if key not in ("children", "childrenUrls")}
    for record in data
]

bronze_df = spark.createDataFrame(cleaned_data)

# I'm saving this permanently as a Delta table, this is my raw, untouched copy
bronze_df.write.mode("overwrite").saveAsTable("bronze_bikepoints")

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The above code completes my "Extract" step. It turns the raw data list into a Spark DataFrame and saves it as a Delta table called bronze_bikepoints, my untouched, permenant copy of what the API gave me.
# MAGIC
# MAGIC The data in the additionalProperties column is still nested, so I will make sure the flattening happens in the next "Transform" step.
# MAGIC
# MAGIC I actually had to drop 'children' and 'childrenUrls' first. Both are always empty lists for every bike point, so Spark couldn't infer a type for them and threw a "CANNOT_DETERMINE_TYPE" error. Since they're empty for every record anyway, dropping felt like the right option and cost me nothing.
# MAGIC
# MAGIC I also ran a .count() below to check the delta table properly. This was because the above table showed that there was 712+ rows and Truncated data which looked like missing rows at first glance. After running it, it returned 800, matching the original API count exactly. So nothing was lost, the "712+" was just Databricks not rendering every row in the preview.
# MAGIC

# COMMAND ----------

print(bronze_df.count())

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Read the untouched Bronze table back in
bronze_df = spark.table("bronze_bikepoints")

# EXPLODE: Now I want to turn each bike point's list of sticky notes into individual rows
exploded_df = bronze_df.select(
    "id",
    "commonName",
    "lat",
    "lon",
    explode("additionalProperties").alias("property")
)

display(exploded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - The above is the first step of my Transform stage. I decided to use the exploding technique because each bike point started as one row with a pile of sticky notes (additionalProperties) squashed together into a single cell, and that's not something I can easily read or work with.
# MAGIC - Explode essentially takes that pile of sticky notes and lays it out flat, turning each note into its own row, while keeping the same id, commonName, lat, and lon attached to every one.
# MAGIC - This is why the same bike point now appears multiple times in a row, once per every sticky note it has. This is the process of me unpacking the sticky note pile.
# MAGIC - Now that every sticky note has its own row, I can finally reach inside each one and pull out the most useful bits, the key (which is the attribute name like NbBikes) and the value (like '17')

# COMMAND ----------

# Reach inside each sticky note and pull out just the two bits I actually need:
# the key (attribute name) and value, dropping the rest as uneeded noise
clean_df = exploded_df.select(
    "id",
    "commonName",
    "lat",
    "lon",
    col("property.key").alias("attribute_name"),
    col("property.value").alias("attribute_value")
)

display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - This particular step allowed me to clean up the exploded data. Each row still had a whole sticky note bundled into one property column, with extra fields I don't need like $type.
# MAGIC - In the above, I reached inside each one and pulled out what I needed (key and value), and renamed them to attribute_name and attribute_value so they're clearer to read. The next step will be for me to move into the pivot area.

# COMMAND ----------


from pyspark.sql.functions import first
# Collapse the rows back into one per bike point. Did this by turning attribute_name values into real columns
silver_df = clean_df.groupBy("id", "commonName", "lat", "lon") \
    .pivot("attribute_name") \
    .agg(first("attribute_value"))

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - This is the pivot step, the second half of flattening.
# MAGIC - I grouped the rows back together by id, commonName, lat, and lon so all attribute rows per bike point collapse into a single group.
# MAGIC - .pivot("attribute_name") took every distinct attribute name and turned each one into its own column header.
# MAGIC - .agg(first("attribute_value")) just grabs the one value belonging to each bike point / column combination, since theres only ever one match per group.
# MAGIC - Result: back down to 800 rows, but genuinely flat, one row per bike point with real columns instead of a bundled list.
# MAGIC - Next, before treating this as my final finished Silver table, I need to check the data quality of the table. I will start off by correcting the column data types.

# COMMAND ----------

# After pivoting, there were multiple columns that came through as text, even the numeric ones. Decided to cast them properly here.
silver_df_typed = silver_df \
    .withColumn("NbBikes", col("NbBikes").cast("int")) \
    .withColumn("NbDocks", col("NbDocks").cast("int")) \
    .withColumn("NbEBikes", col("NbEBikes").cast("int")) \
    .withColumn("NbEmptyDocks", col("NbEmptyDocks").cast("int")) \
    .withColumn("NbStandardBikes", col("NbStandardBikes").cast("int")) \
    .withColumn("Installed", col("Installed").cast("boolean")) \
    .withColumn("Locked", col("Locked").cast("boolean")) \
    .withColumn("Temporary", col("Temporary").cast("boolean"))

display(silver_df_typed)

# COMMAND ----------

# MAGIC %md
# MAGIC - After pivoting, I decided to fix the earlier issue I raised (which was changing the data types to their correct format, '17' as text to 17 as a number for example). There were a number of columns that came through as text, even though they were not (such as NbBikes, NbEBikes, NbDocks ect.)
# MAGIC - I used .withColumn() to overwrite each bike / dock count column, casting it to int so it behaves like a real number instead of text.
# MAGIC - I did the same for Installed, Locked, and Temporary, casting them to boolean so they read as genuine true / false rather than the text "true" / "false"
# MAGIC - RemovalDate is the next issue to look at, as the column appears to only contain blanks.

# COMMAND ----------

#Check every unique value in RemovalDate rather than assume it's all blank.
silver_df_typed.select("RemovalDate").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - I ran the above to check every unique value that appears in the RemovalDate column. I did not want to assume it was all blank from the get go.
# MAGIC - The result showed me one blank value, plus 3 real timestamps. So it can be inferred that most stations have no removal date and are still active, but there are a small number that are not.
# MAGIC - This told me there was something genuinely worth investigating here, rather than a column I could just ignore. My next step will be to find the specific fields that contain non blank values in the RemovalDate column.

# COMMAND ----------

# Look closer at the statopms with a RemovalDate to understand what's going on
from pyspark.sql.functions import col

silver_df_typed.filter(col("RemovalDate") != "") \
    .select("id", "commonName", "Installed", "Locked", "NbDocks", "RemovalDate") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - I dug deeper into the stations with a RemovalDate to understand whether they're actually inactive.
# MAGIC - All 3 show Installed = true and Locked = false, and a healthy NbDocks count. This is all consistent with what an active, working station looks like. With this being said, it contradicts having a RemovalDate at all.
# MAGIC - My most likely explanation is that these stations were removed at some point in the past, and then reinstalled, and TFL's backend team never cleared the old RemovalDate field. 
# MAGIC - However, I can't be certain from the data alone. 
# MAGIC - There's also a possibility that TFL has genuinely decommissoned them and it has not been synced yet.
# MAGIC - In a real world setting, I would conduct further research to understand the real case behind this. Maybe I would be in regular communication with the TfL backend team to gather a more structured diagnosis.
# MAGIC - Since this feeds into a recommendation regarding where the Mayor should invest, I decided to drop these 3 bikepoints rather than assume they're fine. The risk of recommending an investment near a station that might already be on its way out outweighs losing 3 rows out of 800.

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime, when

# I'm splitting out the stations with a RemovalDate value
excluded_stations = silver_df_typed.filter(col("RemovalDate") != "")
silver_df_clean = silver_df_typed.filter(col("RemovalDate") == "")

# Some stations have a blank InstallDate. Convert blanks to a real null first,
# so the cast only ever sees a valid number or a clean null, never an empty string
silver_df_clean = silver_df_clean.withColumn(
    "InstallDate",
    when(col("InstallDate") == "", None).otherwise(col("InstallDate"))
)

# Now I safely convert the Unix millisecond timestamp into an actual date. I noticed that InstallDate was not in a readable format, so this needed to be amended.
silver_df_clean = silver_df_clean.withColumn(
    "InstallDate",
    from_unixtime(col("InstallDate").cast("long") / 1000).cast("date")
)

display(silver_df_clean)

# COMMAND ----------

# Now, to save my cleaned, flattened data as the Silver Delta table
silver_df_clean.write.mode("overwrite").saveAsTable("silver_bikepoints")

# Also save the 3 excluded stations separately, so there's a record of what I removed and why
excluded_stations.write.mode("overwrite").saveAsTable("excluded_bikepoints")

print(f"Silver table saved: {spark.table('silver_bikepoints').count()} rows")
print(f"Excluded stations saved: {spark.table('excluded_bikepoints').count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold step: I'm working out how full or empty each station currently is,
# MAGIC -- so I can spot stations that are struggling to meet demand right now
# MAGIC CREATE OR REPLACE TABLE gold_station_utilization AS
# MAGIC SELECT
# MAGIC     id,
# MAGIC     commonName,
# MAGIC     lat,
# MAGIC     lon,
# MAGIC     NbBikes,
# MAGIC     NbEmptyDocks,
# MAGIC     NbDocks,
# MAGIC     ROUND(NbBikes / NbDocks * 100, 0) AS pct_bikes_available,
# MAGIC     ROUND(NbEmptyDocks / NbDocks * 100, 0) AS pct_docks_available
# MAGIC FROM silver_bikepoints
# MAGIC WHERE NbDocks >  0  -- avoid dividing by zero for any station with no docks at all
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - This is my first Gold table, the first step in my Load stage. I calculated the percentage of bikes and empty docks available at each station right now, to spot the ones under strain. The "no rows returned" display prompted me to want to see if anything landed in the table, or whether this is a display quirk.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking how many rows actually landed in the table
# MAGIC SELECT COUNT(*) AS total_rows FROM gold_station_utilization

# COMMAND ----------

# MAGIC %md
# MAGIC - After checking the row count, I could see the table was built. However, the count came back as 796 and not 797. I wanted to do some further investigation as to why this was the case.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, commonName, NbBikes, NbEmptyDocks, NbDocks
# MAGIC FROM silver_bikepoints
# MAGIC WHERE NbDocks = 0 OR NbDocks IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC - I checked which station my NbDocks > 0 filter had excluded.
# MAGIC - Found BikePoints_612 in Wandsworth road, with 0 values for NbBikes, Empty Docks, and Docks.
# MAGIC - This is a different issue to the RemovalDate problem earlier, as this particular BikePoint has 0 capacity recorded at all.
# MAGIC - With this being said, I believe it is right to exclude it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Surfacing the stations under the most strain right now, so the ones nearly empty of bikes or nearly full of docks
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         id,
# MAGIC         commonName,
# MAGIC         NbBikes,
# MAGIC         NbEmptyDocks,
# MAGIC         NbDocks,
# MAGIC         pct_bikes_available,
# MAGIC         pct_docks_available,
# MAGIC         ROW_NUMBER() OVER (ORDER BY LEAST(pct_bikes_available, pct_docks_available) ASC) AS severity_rank
# MAGIC     FROM gold_station_utilization
# MAGIC )
# MAGIC SELECT
# MAGIC     CONCAT(LPAD(CAST(severity_rank AS STRING), 2, '0'), '. ', commonName) AS station_label,
# MAGIC     pct_bikes_available,
# MAGIC     pct_docks_available
# MAGIC FROM ranked
# MAGIC ORDER BY severity_rank ASC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC - My above query pulls out the top 20 stations currently under the most strain, with either no bikes left to rent, or almost no empty docks left to return to.
# MAGIC - I used LEAST() to sort by whichever of the two percentages (pct_bikes_available & pct_docks_available) is worse for each station, so the single worst-off stations appear right at the top. Both percentages consider the NbBikes, NbEmptyDocks, & NbDocks values to identify worst points.
# MAGIC - **This is a first cut at answering the Mayor's question as the table above outlines the areas with the most strain and thus the ones that need the most investment. However, it's based on one live snapshot. It shows where problems exist right now, not necessarily which stations are chronically struggling over time, a limitation I am flagging here at this point as something I would want to address with more time. I would prefer to address this with repeated, scheduled data pulls.**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Final thoughts on this analysis
# MAGIC
# MAGIC - My final output is a ranked list of the 20 bike points currently under the most strain, ordered from most severe to least severe, presented as a table for a clear, quick read.
# MAGIC - This isn't the final word on where to invest, it's a live snapshot, so it shows where problems exist right now, not which stations are chronically struggling over time. With more time, I'd repeat this pull on a schedule to build up a proper history before treating any single station as a confirmed priority.
# MAGIC - I also haven't factored in cost. The Mayor's office will have a fixed budget, not an unlimited one, so in practice I wouldn't recommend investing in all 20 stations at once regardless of severity. Instead, I'd treat this ranking as a prioritisation tool: start from the top of the list and work downwards, funding as many as the available budget allows.
# MAGIC - A fuller version of this would also weigh each station's expansion cost (which will vary by location, available space, and how much extra capacity is needed) against its severity rank. A station near the top of the list but very expensive to expand might reasonably be deprioritised below a slightly less severe but much cheaper one. I didn't have cost data to do that here, but it's the natural next step.
# MAGIC - **My recommendation to the Mayor: start with the most severe bike point, East Road, Hoxton, and work down the list from there. This priority order should guide where investment goes first, but real budget and expansion costs still need checking before committing to anything. I'd also want to confirm severity over time with repeated data pulls, rather than relying on this one snapshot.**

# COMMAND ----------

