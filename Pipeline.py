# Databricks notebook source
import pandas as pd
import math

# File path
user_path = "/Workspace/Users/arinsang@ais.co.th"
inputPath = f"{user_path}/input/track_small.csv"
outputPath = f"{user_path}/output/output_small.csv"

# Extract
tracks = pd.read_csv(inputPath)

# Transform
tracks["UnitPrice"] = tracks["UnitPrice"].apply(lambda x: math.ceil(x))
                             
# Load
tracks.to_csv(outputPath, index=False)

# COMMAND ----------

# MAGIC %sh
# MAGIC head -5 /Workspace/Users/arinsang@ais.co.th/input/track_small.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC head -5 /Workspace/Users/arinsang@ais.co.th/output/output_small.csv

# COMMAND ----------

import pandas as pd
from datetime import datetime

# file path
inputPath = f"{user_path}/input/track_small.csv"
outputPath = f"{user_path}/output/output_small.csv"
testResultPath = f"{user_path}/result/test_result.txt"

# read files
tracksInput = pd.read_csv(inputPath)
tracksOutput = pd.read_csv(outputPath)

# open test result file
f = open(testResultPath, "a")

# write datetime
f.write(datetime.now().strftime("%d-%m-%Y %H:%M:%S") + '\n')

# Case 1
unitPriceType = tracksOutput.dtypes['UnitPrice']
if unitPriceType == 'int64':
    f.write("Case 1: Pass\n")
else:
    f.write("Case 1: Fail\n")

# # Case 2
mergedTracks = pd.merge(tracksInput, tracksOutput, on='TrackId', suffixes=('_input', '_output'))
if (mergedTracks['UnitPrice_output'] - mergedTracks['UnitPrice_input'] < 1).all():
    f.write("Case 2: Pass\n")
else:
    f.write("Case 2: Fail\n")

# # close test result file
# f.close()

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /Workspace/Users/arinsang@ais.co.th/result/test_result.txt
