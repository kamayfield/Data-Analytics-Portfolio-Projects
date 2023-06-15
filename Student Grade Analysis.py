# Databricks notebook source
# MAGIC %md
# MAGIC # DSCI 417 - Project 02
# MAGIC ## Student Grade Database
# MAGIC **Kyle Mayfield**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: Set up Environment

# COMMAND ----------

# MAGIC %md
# MAGIC In this part of the project, we will set up our environment. We will begin with some import statements.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
 
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: Load the data

# COMMAND ----------

# MAGIC %md
# MAGIC We will now import each data file into a Spark DataFrame.

# COMMAND ----------

accepted = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema(
        'acc_term_id STRING, sid INTEGER, first_name STRING, last_name STRING, major STRING'
    )
    .csv('/FileStore/tables/univ/accepted.csv')
)

alumni = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('sid INTEGER')
    .csv('/FileStore/tables/univ/alumni.csv')
)

courses = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('dept STRING, course STRING, prereq STRING, credits STRING')
    .csv('/FileStore/tables/univ/courses.csv')
)

expelled = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('sid INTEGER')
    .csv('/FileStore/tables/univ/expelled.csv')
)

faculty = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('fid INTEGER, first_name STRING, last_name STRING, dept STRING')
    .csv('/FileStore/tables/univ/faculty.csv')
)

grades = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('term_id STRING, course STRING, sid INTEGER, fid INTEGER, grade STRING')
    .csv('/FileStore/tables/univ/grades.csv')
)

unretained = (
    spark.read
    .option('delimiter', ',')
    .option('header', True)
    .schema('sid INTEGER')
    .csv('/FileStore/tables/univ/unretained.csv')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will print the number of records in each DataFrame.

# COMMAND ----------

print(f'The numer of records in accepted is {accepted.count()}')
print(f'The numer of records in alumni is {alumni.count()}')
print(f'The numer of records in courses is {courses.count()}')
print(f'The numer of records in expelled is {expelled.count()}')
print(f'The numer of records in faculty is {faculty.count()}')
print(f'The numer of records in grades is {grades.count()}')
print(f'The numer of records in unretained is {unretained.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: Student Count by Status

# COMMAND ----------

# MAGIC %md
# MAGIC We will now create a three new DataFrames to store student info for students in various categories. We will then generate the desired counts.

# COMMAND ----------

enrolled = (
    accepted
    .join(grades, 'sid', 'semi')
)

current = (
    enrolled
    .join(alumni, 'sid', 'anti')
    .join(unretained, 'sid', 'anti')
    .join(expelled, 'sid', 'anti')
)

former = (
    enrolled
    .join(current, 'sid', 'anti')
)

print(f'Number of accepted students:   {accepted.count()}')
print(f'Number of enrolled students:   {enrolled.count()}')
print(f'Number of current students:    {current.count()}')
print(f'Number of former students:     {former.count()}')
print(f'Number of unretained students: {unretained.count()}')
print(f'Number of expelled students:   {expelled.count()}')
print(f'Number of alumni:              {alumni.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: Distribution of Students by Major

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, we will determine of the number of students currently in each major, as well as the proportion of
# MAGIC the overall number of students in each major.

# COMMAND ----------

current_students = current.count()

(
    current
    .groupBy('major')
    .agg(expr('count(*) as n_students'))
    .withColumn('prop', expr(f'round(n_students / {current_students}, 4)'))
    .sort('prop', ascending = False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E: Course Enrollments by Department

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, we will determine of the number of students enrolled in courses offered by each department during
# MAGIC the Spring 2021 term.

# COMMAND ----------

sp21_enr = grades.filter(expr('term_id == "2021A"')).count()

(
    grades
    .filter(expr('term_id = "2021A"'))
    .join(courses, 'course')
    .groupBy('dept')
    .agg(expr('count(*) as n_students'))
    .withColumn('prop', expr(f'round(n_students / {sp21_enr}, 4)'))
    .sort('prop', ascending = False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part F: Graduation Rates by Major

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, we will determine the graduation rates for each major. We will perform this analysis in steps. First,
# MAGIC we will create a DataFrame containing the number of former students in each major. Then we will create a
# MAGIC DataFrame containing the number of alumni for each major. We will then combine these DataFrames to
# MAGIC determine the graduation rate.

# COMMAND ----------

(
    former
    .groupBy('major')
    .agg(expr('count(*) as n_former'))
    .sort('major')
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now determine the number of alumni for each major.

# COMMAND ----------

(
    former
    .join(alumni, 'sid', 'semi')
    .groupBy('major')
    .agg(expr('count(*) as n_alumni'))
    .sort('major')
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now use the previous two DataFrames to determine the graduation rates

# COMMAND ----------

former_by_major =  (   
    former
    .groupBy('major')
    .agg(expr('count(*) as n_former'))
    .sort('major')
)

alumni_by_major = (
    former
    .join(alumni, 'sid', 'semi')
    .groupBy('major')
    .agg(expr('count(*) as n_alumni'))
    .sort('major')
)

(
    former_by_major
    .join(alumni_by_major, 'major')
    .select(
        'major', 'n_alumni', 'n_former', expr('round((n_alumni / n_former), 4) as grad_rate')
    )
    .sort('major')
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part G: Number of Terms Required for Graduation

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, we will find a frequency distribution for the number of terms that alumni required for graduation.

# COMMAND ----------

(
    grades
    .join(alumni, 'sid', 'semi')
    .groupBy('sid')
    .agg(expr('count(distinct term_id) as n_terms'))
    .groupBy('n_terms')
    .agg(expr('count(*) as n_alumni'))
    .sort('n_terms')
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part H: Current Student GPA

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, we will calculate the GPA of each current student at SU and will analyze the results.

# COMMAND ----------

def conv_letter(x):
   letter_dict = {'A': 4, 'B': 3, 'C': 2, 'D': 1, 'F': 0}
   return letter_dict.get(x)


spark.udf.register('conv_letter', conv_letter)

# COMMAND ----------

current_gpa = (
    grades
    .join(courses, 'course')
    .withColumn('num_grade', expr('conv_letter(grade)'))
    .withColumn('gp', expr('credits * num_grade'))
    .groupBy('sid')
    .agg(
        expr('sum(gp) as total_gp'),
        expr('sum(credits) as total_credits')
    )
    .withColumn('gpa', expr('round((total_gp / total_credits), 2)'))
    .join(current, 'sid')
    .select('sid', 'first_name', 'last_name', 'major', 'gpa')
    .sort('gpa')
                
)

current_gpa.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now determine the number of current students with perfect 4.0 GPAs.

# COMMAND ----------

(
    current_gpa
    .filter(expr('gpa == 4.0'))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will create a histogram displaying the distribution of GPAs for current students.

# COMMAND ----------

current_gpa_pdf = current_gpa.toPandas()

plt.hist(current_gpa_pdf.gpa, bins = [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 3.75, 4.0], color = 'orange', edgecolor = 'black')
plt.title('GPA Distribution for Current Students')
plt.xlabel('GPA')
plt.ylabel('Count')
 
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part I: Grade Distribution by Instructor

# COMMAND ----------

# MAGIC %md
# MAGIC In this part, we will determine the proportion of A, B, C, D, and F grades given out by each faculty member at SU.

# COMMAND ----------

faculty_grade_dist = (
    grades
    .groupBy('fid')
    .agg(
        expr('count(*) as N'),
        expr('sum(case when grade == "A" then 1 else 0 end) as countA'),
        expr('sum(case when grade == "B" then 1 else 0 end) as countB'),
        expr('sum(case when grade == "C" then 1 else 0 end) as countC'),
        expr('sum(case when grade == "D" then 1 else 0 end) as countD'),
        expr('sum(case when grade == "F" then 1 else 0 end) as countF')
    )
    .join(faculty, 'fid')
    .select(
        'fid', 'first_name', 'last_name', 'dept', 'N', 
        expr('round((countA / N), 2) as propA'),
        expr('round((countB / N), 2) as propB'),
        expr('round((countC / N), 2) as propC'),
        expr('round((countD / N), 2) as propD'),
        expr('round((countF / N), 2) as propF')
        )
)

faculty_grade_dist.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now identify the 10 faculty members who assign the fewest A grades. 

# COMMAND ----------

faculty_grade_dist.filter(expr('N >= 100')).sort('propA').show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now identify the 10 faculty members who award A’s most frequently.

# COMMAND ----------

faculty_grade_dist.filter(expr('N >= 100')).sort('propA', ascending = False).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part J: First Term GPA

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, we calculate the first-term GPA for each student who has enrolled in classes at SU.

# COMMAND ----------

first_term_gpa = (
    grades
    .join(accepted, 'sid')
    .filter(expr('term_id == acc_term_id'))
    .join(courses, 'course')
    .withColumn('num_grade', expr('conv_letter(grade)'))
    .withColumn('gp', expr('credits * num_grade'))
    .groupBy('sid')
    .agg(
        expr('sum(gp) as total_gp'),
        expr('sum(credits) as total_credits')
    )
    .withColumn('first_term_gpa', expr('round((total_gp / total_credits), 2)'))
    .select('sid', 'first_term_gpa')
)

first_term_gpa.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part K: Graduation Rates and First Term GPA

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, we will calculate graduation rates for students whose first term GPA falls into each of four different grade ranges.

# COMMAND ----------

def grade_bin(x):
    bins = ['[0,1)', '[1,2)', '[2,3)', '[3,4]']
    return bins[int(x - 1)]

spark.udf.register('grade_bin', grade_bin)

# COMMAND ----------

# MAGIC %md
# MAGIC We will now calculate the number of alumni whose first-term GPA falls into each bin.

# COMMAND ----------

alumni_ft_gpa = (
    first_term_gpa
    .join(alumni, 'sid', 'semi')
    .withColumn('gpa_bin', expr('grade_bin(first_term_gpa)'))
    .groupBy('gpa_bin')
    .agg(expr('count(*) as n_alumni'))
    .sort('gpa_bin')
)

alumni_ft_gpa.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will determine the number of former students whose first-term GPA falls into each bin.

# COMMAND ----------

former_ft_gpa = (
    first_term_gpa
    .join(former, 'sid', 'semi')
    .withColumn('gpa_bin', expr('grade_bin(first_term_gpa)'))
    .groupBy('gpa_bin')
    .agg(expr('count(*) as n_former'))
    .sort('gpa_bin')
)

former_ft_gpa.show()

# COMMAND ----------

(
    alumni_ft_gpa
    .join(former_ft_gpa, 'gpa_bin')
    .withColumn('grad_rate', expr('round((n_alumni / n_former), 4)'))
    .sort('gpa_bin')
    .show()
)
