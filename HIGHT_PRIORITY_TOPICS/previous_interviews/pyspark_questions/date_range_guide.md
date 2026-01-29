# Generating Weekdays and Weekends: Complete Guide

**SQL and PySpark Solutions for Dynamic Date Generation**

-----

## Table of Contents

1. [Problem Statement](#problem-statement)
1. [PostgreSQL Solution](#postgresql-solution)
- [Complete Query](#complete-query)
- [Detailed Line-by-Line Explanation](#detailed-line-by-line-explanation)
- [Execution Flow](#execution-flow)
1. [PySpark Solution](#pyspark-solution)
- [Complete Code](#complete-code)
- [Detailed Line-by-Line Explanation](#detailed-line-by-line-explanation-1)
- [Execution Flow](#execution-flow-1)
1. [Understanding Date Ranges](#understanding-date-ranges)
- [Why -30 and +30 Days?](#why--30-and-30-days)
- [The Purpose of Date Windows](#the-purpose-of-date-windows)
- [Customizing Date Ranges](#customizing-date-ranges)
1. [Real-World Use Cases](#real-world-use-cases)
1. [Advanced Examples](#advanced-examples)
1. [Quick Reference](#quick-reference)

-----

## Problem Statement

**Goal:** Generate a complete list of all dates (Monday through Sunday) within a date range, and classify each date as either “Weekday” or “Weekend” - **without hardcoding any dates**.

**Requirements:**

- Generate dates dynamically (based on current date)
- Include all days (no gaps)
- Show day name (Monday, Tuesday, etc.)
- Show day of week as number
- Classify as Weekday or Weekend
- Order chronologically

**Key Challenge:** The solution must be completely dynamic - no hardcoded dates!

-----

## PostgreSQL Solution

### Complete Query

```sql
WITH RECURSIVE DateSeries AS (
    -- Base case: Start date (30 days ago)
    SELECT CURRENT_DATE - INTERVAL '30 days' AS date
    
    UNION ALL
    
    -- Recursive case: Add one day
    SELECT date + INTERVAL '1 day'
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'
)
SELECT 
    date,
    TO_CHAR(date, 'Day') AS day_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM DateSeries
ORDER BY date;
```

-----

### Detailed Line-by-Line Explanation

#### Line 1: Declare Recursive CTE

```sql
WITH RECURSIVE DateSeries AS (
```

**Breaking it down:**

- `WITH`: Starts a Common Table Expression (CTE) definition
- `RECURSIVE`: Keyword indicating this CTE will reference itself (recursion)
- `DateSeries`: Name we give to this temporary result set
- `AS (`: Begins the CTE definition

**Purpose:** Declares that we’re creating a self-referencing temporary result set called `DateSeries` that will generate our sequence of dates.

**Think of it as:** Creating a “virtual table” that can call itself to build data step by step.

-----

#### Lines 2-3: Base Case (Starting Point)

```sql
    -- Base case: Start date (30 days ago)
    SELECT CURRENT_DATE - INTERVAL '30 days' AS date
```

**Breaking it down:**

**`SELECT`**

- Standard SQL SELECT statement

**`CURRENT_DATE`**

- PostgreSQL function that returns today’s date (without time component)
- Example: If today is February 15, 2026, returns `2026-02-15`
- This makes the query **dynamic** (no hardcoded dates!)

**`- INTERVAL '30 days'`**

- `INTERVAL`: PostgreSQL data type for time spans/durations
- `'30 days'`: String literal specifying the duration
- The minus operator subtracts this interval from the date
- Example: `2026-02-15` - 30 days = `2026-01-16`

**`AS date`**

- Aliases the result column as “date”
- This column name will be used throughout the query

**Purpose:** This is the **anchor/base case** of the recursion. It defines where our date sequence starts - 30 days before today. This executes **once** and produces **one row** with the starting date.

**What happens:**

```
If today is: 2026-02-15
Base case produces: 2026-01-16
```

-----

#### Line 5: Union Operator

```sql
    UNION ALL
```

**Breaking it down:**

- `UNION ALL`: Combines results from the base case with results from the recursive case
- `ALL`: Keeps all rows including duplicates (no duplicate checking)
- This is the **connector** between base case and recursive iterations

**Why UNION ALL and not UNION?**

- `UNION` removes duplicates → slower (requires sorting/comparison)
- `UNION ALL` keeps everything → faster
- In date generation, we won’t have duplicates anyway
- For large date ranges, performance difference is significant

**Purpose:** Links the starting point (base case) with the recursive iterations that will follow. Essential for recursive CTEs to work.

-----

#### Lines 7-10: Recursive Case (Generate Next Dates)

```sql
    -- Recursive case: Add one day
    SELECT date + INTERVAL '1 day'
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'
```

**Line-by-line breakdown:**

**`SELECT date + INTERVAL '1 day'`**

- Takes the `date` column from the previous iteration
- Adds exactly one day to it
- `+ INTERVAL '1 day'`: Adds 24 hours
- Example: If previous date was `2026-01-16`, this produces `2026-01-17`

**`FROM DateSeries`** - **THIS IS THE MAGIC!**

- References the CTE itself (`DateSeries`)
- Creates the **recursion** - the CTE calls itself
- Reads the results from the previous iteration
- On iteration 1: reads the base case result (2026-01-16)
- On iteration 2: reads iteration 1 results (2026-01-17)
- On iteration N: reads iteration N-1 results

**`WHERE date < CURRENT_DATE + INTERVAL '30 days'`** - **TERMINATION CONDITION**

- `CURRENT_DATE + INTERVAL '30 days'`: Calculates end date
  - Example: `2026-02-15` + 30 days = `2026-03-17`
- `date <`: Continues only if current date is **less than** end date
- When this condition becomes **FALSE**, recursion **STOPS**
- This prevents infinite loops

**Purpose:** This is the **recursive case** that executes repeatedly, adding one day at a time, until we reach the end date. Each iteration uses the results from the previous iteration to generate the next date.

**Visual Flow:**

```
Iteration 0 (Base): 2026-01-16
                        ↓ (+1 day)
Iteration 1 (Recursive): 2026-01-17
                        ↓ (+1 day)
Iteration 2 (Recursive): 2026-01-18
                        ↓ (+1 day)
                       ...
                        ↓ (+1 day)
Iteration 59 (Recursive): 2026-03-16
                        ↓ (+1 day)
Iteration 60 (Recursive): 2026-03-17
                        ↓
              Check: 2026-03-17 < 2026-03-17? NO → STOP
```

-----

#### How Recursion Works: Complete Execution Flow

Let’s trace through the recursion step by step (assuming today is 2026-02-15):

**Iteration 0 (Base Case):**

```
Execute: SELECT CURRENT_DATE - INTERVAL '30 days'
Result: 2026-01-16
Check: (not applicable for base case)
Status: ✅ Add to result set
```

**Iteration 1 (First Recursive Call):**

```
Input from previous: 2026-01-16
Execute: SELECT date + INTERVAL '1 day' FROM DateSeries
Result: 2026-01-16 + 1 day = 2026-01-17
Check: 2026-01-17 < 2026-03-17? YES
Status: ✅ Add to result set, continue recursion
```

**Iteration 2:**

```
Input from previous: 2026-01-17
Result: 2026-01-17 + 1 day = 2026-01-18
Check: 2026-01-18 < 2026-03-17? YES
Status: ✅ Add to result set, continue recursion
```

**… (continues for each day) …**

**Iteration 59:**

```
Input from previous: 2026-03-16
Result: 2026-03-16 + 1 day = 2026-03-17
Check: 2026-03-17 < 2026-03-17? NO (not less than, equal)
Status: ❌ STOP RECURSION
```

**Final Result:**

```
Total iterations: 61 (0 through 60)
Total rows: 61 (from 2026-01-16 to 2026-03-17, inclusive)
Date range: 61 days
```

-----

#### Lines 12-21: Final SELECT - Format and Display Results

```sql
SELECT 
    date,
    TO_CHAR(date, 'Day') AS day_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM DateSeries
ORDER BY date;
```

Now we take the generated dates and add useful information.

-----

**Line 13: Select the date**

```sql
    date,
```

- Simply selects the date column from our `DateSeries` CTE
- Example output: `2026-01-16`

-----

**Line 14: Format day name**

```sql
    TO_CHAR(date, 'Day') AS day_name,
```

**Breaking it down:**

- `TO_CHAR`: PostgreSQL function to convert values to formatted strings
- `date`: The date value to format
- `'Day'`: Format pattern that outputs full day name with padding
- `AS day_name`: Names the output column

**Format pattern options:**

- `'Day'` → `'Friday   '` (full name, padded to 9 characters)
- `'Dy'` → `'Fri'` (abbreviated, 3 letters)
- `'D'` → `'6'` (day of week as number, 1-7)
- `'day'` → `'friday'` (lowercase)
- `'DAY'` → `'FRIDAY'` (uppercase)

**Example output:** `'Wednesday'`

-----

**Line 15: Extract day of week as number**

```sql
    EXTRACT(DOW FROM date) AS day_of_week,
```

**Breaking it down:**

- `EXTRACT`: PostgreSQL function to extract components from date/time values
- `DOW`: “Day Of Week” - extracts the day of week as an integer
- `FROM date`: The date to extract from
- `AS day_of_week`: Column alias

**Important:** Returns 0-6 where:

- 0 = Sunday
- 1 = Monday
- 2 = Tuesday
- 3 = Wednesday
- 4 = Thursday
- 5 = Friday
- 6 = Saturday

**Example:** Wednesday returns `3`

-----

**Lines 16-19: Classify as Weekday or Weekend**

```sql
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
```

This is a CASE statement (conditional logic, like if-else):

**`CASE`**

- Starts the conditional expression

**`WHEN EXTRACT(DOW FROM date) IN (0, 6)`**

- Condition to check
- `EXTRACT(DOW FROM date)`: Gets day of week (0-6)
- `IN (0, 6)`: Checks if the value is 0 (Sunday) OR 6 (Saturday)
- Returns TRUE for weekends

**`THEN 'Weekend'`**

- If the condition is TRUE, return the string `'Weekend'`

**`ELSE 'Weekday'`**

- If the condition is FALSE (days 1-5), return `'Weekday'`

**`END`**

- Closes the CASE statement

**`AS day_type`**

- Names this calculated column

**Logic table:**

```
Day of Week | Number | IN (0, 6)? | Result
------------|--------|------------|--------
Sunday      | 0      | YES        | Weekend
Monday      | 1      | NO         | Weekday
Tuesday     | 2      | NO         | Weekday
Wednesday   | 3      | NO         | Weekday
Thursday    | 4      | NO         | Weekday
Friday      | 5      | NO         | Weekday
Saturday    | 6      | YES        | Weekend
```

-----

**Line 20: Specify data source**

```sql
FROM DateSeries
```

- Specifies that we’re querying from the `DateSeries` CTE we defined above
- At this point, `DateSeries` contains all 61 dates from the recursive generation

-----

**Line 21: Sort results**

```sql
ORDER BY date;
```

- `ORDER BY`: SQL clause to sort results
- `date`: Sort by the date column
- Default is **ascending** order (earliest date first)
- `;`: Ends the SQL statement

**Note:** In this case, dates are already in order from the generation process, but `ORDER BY` ensures it explicitly.

-----

### Complete Execution Visualization

Let me show you the complete flow with actual data:

```
┌─────────────────────────────────────────────────────┐
│ STEP 1: Build DateSeries CTE (Recursive)           │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Base Case (Iteration 0):                           │
│   SELECT CURRENT_DATE - INTERVAL '30 days'         │
│   Result: 2026-01-16                               │
│                                                     │
│           UNION ALL                                 │
│                                                     │
│ Recursive Case (Iteration 1):                      │
│   SELECT 2026-01-16 + INTERVAL '1 day'             │
│   WHERE 2026-01-16 < 2026-03-17 ✅                 │
│   Result: 2026-01-17                               │
│                                                     │
│           UNION ALL                                 │
│                                                     │
│ Recursive Case (Iteration 2):                      │
│   SELECT 2026-01-17 + INTERVAL '1 day'             │
│   WHERE 2026-01-17 < 2026-03-17 ✅                 │
│   Result: 2026-01-18                               │
│                                                     │
│ ... (continues) ...                                 │
│                                                     │
│ Recursive Case (Iteration 60):                     │
│   SELECT 2026-03-16 + INTERVAL '1 day'             │
│   WHERE 2026-03-17 < 2026-03-17 ❌ STOP            │
│                                                     │
│ DateSeries now contains 61 rows:                   │
│   [2026-01-16, 2026-01-17, ..., 2026-03-17]        │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 2: Final SELECT Processes Each Row            │
├─────────────────────────────────────────────────────┤
│                                                     │
│ For each date in DateSeries:                       │
│   ├─ date: Keep as-is                              │
│   ├─ TO_CHAR(date, 'Day'): Format as day name      │
│   ├─ EXTRACT(DOW): Get day of week number          │
│   └─ CASE: Classify as Weekday or Weekend          │
│                                                     │
│ Process date: 2026-01-16                           │
│   ├─ date: 2026-01-16                              │
│   ├─ day_name: 'Friday'                            │
│   ├─ day_of_week: 5                                │
│   └─ day_type: 'Weekday' (5 not in [0,6])         │
│                                                     │
│ Process date: 2026-01-17                           │
│   ├─ date: 2026-01-17                              │
│   ├─ day_name: 'Saturday'                          │
│   ├─ day_of_week: 6                                │
│   └─ day_type: 'Weekend' (6 in [0,6])             │
│                                                     │
│ ... (continues for all 61 dates) ...               │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 3: ORDER BY date (Final Output)               │
├─────────────────────────────────────────────────────┤
│ date       | day_name  | day_of_week | day_type    │
│------------|-----------|-------------|-------------│
│ 2026-01-16 | Friday    | 5           | Weekday     │
│ 2026-01-17 | Saturday  | 6           | Weekend     │
│ 2026-01-18 | Sunday    | 0           | Weekend     │
│ 2026-01-19 | Monday    | 1           | Weekday     │
│ 2026-01-20 | Tuesday   | 2           | Weekday     │
│ ...        | ...       | ...         | ...         │
│ 2026-03-15 | Sunday    | 0           | Weekend     │
│ 2026-03-16 | Monday    | 1           | Weekday     │
│ 2026-03-17 | Tuesday   | 2           | Weekday     │
└─────────────────────────────────────────────────────┘
```

-----

### Example with Specific Date

Let’s trace through with **current date = February 15, 2026**:

**Calculate Date Range:**

```
Current Date: 2026-02-15

Start Date = 2026-02-15 - 30 days = 2026-01-16
End Date   = 2026-02-15 + 30 days = 2026-03-17

Total days: 61 days (January 16 to March 17, inclusive)
```

**Visual Calendar:**

```
                JANUARY 2026
    Mon  Tue  Wed  Thu  Fri  Sat  Sun
                  1    2    3    4
     5    6    7    8    9   10   11
    12   13   14   15  [16] [17] [18]  ← START
   [19] [20] [21] [22] [23] [24] [25]
   [26] [27] [28] [29] [30] [31]


               FEBRUARY 2026
    Mon  Tue  Wed  Thu  Fri  Sat  Sun
                                [1]
    [2]  [3]  [4]  [5]  [6]  [7]  [8]
    [9] [10] [11] [12] [13] [14] 《15》 ← TODAY
   [16] [17] [18] [19] [20] [21] [22]
   [23] [24] [25] [26] [27] [28]


                MARCH 2026
    Mon  Tue  Wed  Thu  Fri  Sat  Sun
                                [1]
    [2]  [3]  [4]  [5]  [6]  [7]  [8]
    [9] [10] [11] [12] [13] [14] [15]
   [16] [17]  18   19   20   21   22  ← END
    23   24   25   26   27   28   29
    30   31

Legend: [##] = In our range, 《##》= Today
```

**Sample Output:**

```
date       | day_name  | day_of_week | day_type
-----------|-----------|-------------|----------
2026-01-16 | Friday    | 5           | Weekday
2026-01-17 | Saturday  | 6           | Weekend
2026-01-18 | Sunday    | 0           | Weekend
2026-01-19 | Monday    | 1           | Weekday
2026-01-20 | Tuesday   | 2           | Weekday
2026-01-21 | Wednesday | 3           | Weekday
2026-01-22 | Thursday  | 4           | Weekday
2026-01-23 | Friday    | 5           | Weekday
2026-01-24 | Saturday  | 6           | Weekend
2026-01-25 | Sunday    | 0           | Weekend
...
2026-02-15 | Sunday    | 0           | Weekend  ← TODAY
...
2026-03-15 | Sunday    | 0           | Weekend
2026-03-16 | Monday    | 1           | Weekday
2026-03-17 | Tuesday   | 2           | Weekday
```

-----

## PySpark Solution

### Complete Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek, when
from datetime import datetime, timedelta

# Create Spark session
spark = SparkSession.builder.appName("WeekdaysWeekends").getOrCreate()

# Calculate date range dynamically
start_date = datetime.now().date() - timedelta(days=30)  # 30 days ago
end_date = datetime.now().date() + timedelta(days=30)    # 30 days ahead

# Generate date sequence using SQL
dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) AS date
""")

# Add day information
result = dates.withColumn("day_name", date_format(col("date"), "EEEE")) \
              .withColumn("day_of_week", dayofweek(col("date"))) \
              .withColumn("day_type", 
                         when(col("day_of_week").isin(1, 7), "Weekend")
                         .otherwise("Weekday")) \
              .orderBy("date")

# Display results
result.show(20, truncate=False)
```

-----

### Detailed Line-by-Line Explanation

#### Lines 1-3: Import Required Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek, when
from datetime import datetime, timedelta
```

**Line 1: `from pyspark.sql import SparkSession`**

- `from pyspark.sql`: PySpark’s SQL module
- `import SparkSession`: Main entry point for PySpark functionality
- **Purpose:** SparkSession is required to create DataFrames, execute SQL queries, and perform all Spark operations

**Line 2: `from pyspark.sql.functions import ...`**

- `col`: Function to reference DataFrame columns in expressions
- `date_format`: Function to format dates as strings (like SQL TO_CHAR)
- `dayofweek`: Function to extract day of week as number (1-7)
- `when`: Function for conditional logic (like SQL CASE WHEN)
- **Purpose:** These are built-in PySpark functions for data transformation and manipulation

**Line 3: `from datetime import datetime, timedelta`**

- `datetime`: Python class for working with dates and times
- `timedelta`: Class for representing time differences/durations
- **Purpose:** Used to dynamically calculate start and end dates (making the solution non-hardcoded)

-----

#### Line 6: Create Spark Session

```python
spark = SparkSession.builder.appName("WeekdaysWeekends").getOrCreate()
```

**Breaking it down:**

**`SparkSession`**

- The main class representing the entry point to Spark functionality
- Required for all Spark operations

**`.builder`**

- Builder pattern to configure the session
- Returns a `SparkSession.Builder` object for chaining configurations

**`.appName("WeekdaysWeekends")`**

- Sets the application name
- `"WeekdaysWeekends"`: Name that appears in Spark UI for monitoring
- Useful for tracking and debugging in cluster environments

**`.getOrCreate()`**

- Checks if a SparkSession already exists
- If yes: returns the existing session (reuse)
- If no: creates a new one with the specified configuration
- Prevents creating duplicate sessions (saves resources)

**Result:** Creates or retrieves a SparkSession object and stores it in variable `spark`

**Why this is important:** Without SparkSession, you cannot create DataFrames or run distributed computations.

-----

#### Lines 9-10: Calculate Date Range Dynamically

```python
start_date = datetime.now().date() - timedelta(days=30)  # 30 days ago
end_date = datetime.now().date() + timedelta(days=30)    # 30 days ahead
```

**Line 9: Calculate Start Date**

**`datetime.now()`**

- Python function that returns current date **and** time
- Example: `2026-02-15 14:23:45.123456`
- Includes: year, month, day, hour, minute, second, microsecond

**`.date()`**

- Method that extracts just the date part (removes time)
- Example: `2026-02-15` (only the date)
- Returns a `date` object (not `datetime`)

**`- timedelta(days=30)`**

- `timedelta(days=30)`: Creates a time duration of 30 days
- The minus operator subtracts this duration from the date
- Arithmetic operation on date objects
- Example: `2026-02-15` - 30 days = `2026-01-16`

**Result:** `start_date = date(2026, 1, 16)` (Python date object)

-----

**Line 10: Calculate End Date**

Same logic as start_date, but **adds** instead of subtracts:

**`datetime.now().date()`**

- Gets today’s date: `2026-02-15`

**`+ timedelta(days=30)`**

- Adds 30 days to the current date
- Example: `2026-02-15` + 30 days = `2026-03-17`

**Result:** `end_date = date(2026, 3, 17)` (Python date object)

-----

**Why use Python datetime instead of hardcoding?**

1. **Dynamic:** Automatically adjusts based on when the script runs
1. **Flexible:** Can easily change the range (change 30 to any number)
1. **Maintainable:** No need to update dates manually
1. **Reusable:** Works for any date range calculation

**Visual representation:**

```
Python Calculation:
┌──────────────────────────────────────────────┐
│ today = datetime.now().date()                │
│ → 2026-02-15                                 │
│                                              │
│ start_date = today - timedelta(days=30)      │
│ → 2026-02-15 - 30 days                       │
│ → 2026-01-16                                 │
│                                              │
│ end_date = today + timedelta(days=30)        │
│ → 2026-02-15 + 30 days                       │
│ → 2026-03-17                                 │
└──────────────────────────────────────────────┘
```

-----

#### Lines 13-18: Generate Date Sequence with Spark SQL

```python
dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) AS date
""")
```

**Line 13: `dates = spark.sql(f"""...""")`**

**`dates =`**

- Variable to store the resulting DataFrame

**`spark.sql(...)`**

- Method to execute SQL queries in Spark
- `spark`: The SparkSession we created earlier
- `.sql()`: Method that takes SQL string and returns DataFrame
- Returns a DataFrame (distributed dataset)

**`f"""..."""`**

- **F-string** (formatted string literal)
- `f` prefix: Allows embedding Python variables using `{}`
- `"""`: Triple quotes for multi-line strings (better readability)
- Example: `f"Hello {name}"` → `"Hello Alice"`

-----

**Line 14: `SELECT explode(sequence(...)) AS date`**

This is where the magic happens! Let’s break down from inside out:

**1. `sequence(start, end, step)` - Generate Array**

- Spark SQL function that generates an array of values
- Creates a sequence from start to end with specified step
- Returns an **array** (all dates in one row)

**Parameters:**

**a) `to_date('{start_date}')`** - First parameter (start)

- `'{start_date}'`: F-string embeds the Python variable
- Becomes: `'2026-01-16'` (string literal)
- `to_date(...)`: Spark function to parse string as date
- Converts string to proper date type

**b) `to_date('{end_date}')`** - Second parameter (end)

- Similar to start_date
- Becomes: `to_date('2026-03-17')`
- End date for the sequence

**c) `interval 1 day`** - Third parameter (step)

- Defines the increment between values
- `interval 1 day`: Add one day at a time
- Could be: `interval 2 days`, `interval 1 hour`, etc.

**What `sequence()` produces:**

```sql
sequence(to_date('2026-01-16'), to_date('2026-03-17'), interval 1 day)

Returns a SINGLE ROW with an ARRAY:
[2026-01-16, 2026-01-17, 2026-01-18, ..., 2026-03-17]
```

-----

**2. `explode(...)` - Convert Array to Rows**

- Spark SQL function that “explodes” an array into multiple rows
- Takes an array and creates one row per array element
- Essentially “unpacks” the array

**What `explode()` does:**

```
Input (1 row):
┌──────────────────────────────────────────────────┐
│ array: [2026-01-16, 2026-01-17, ..., 2026-03-17] │
└──────────────────────────────────────────────────┘

Output (61 rows):
┌────────────┐
│ 2026-01-16 │
│ 2026-01-17 │
│ 2026-01-18 │
│    ...     │
│ 2026-03-17 │
└────────────┘
```

-----

**3. `AS date` - Name the Column**

- Aliases the output column as “date”
- This column can now be referenced as `date` in subsequent operations

-----

**Complete Visual Flow:**

```
Step 1: Python calculates dates
   start_date = 2026-01-16
   end_date = 2026-03-17
            ↓
Step 2: F-string interpolation
   f"to_date('{start_date}')" → to_date('2026-01-16')
   f"to_date('{end_date}')"   → to_date('2026-03-17')
            ↓
Step 3: sequence() generates array
   [2026-01-16, 2026-01-17, 2026-01-18, ..., 2026-03-17]
            ↓
Step 4: explode() converts to rows
   Row 1: 2026-01-16
   Row 2: 2026-01-17
   Row 3: 2026-01-18
   ...
   Row 61: 2026-03-17
            ↓
Step 5: Store in DataFrame
   dates DataFrame with 61 rows, 1 column ("date")
```

**Result:** `dates` is now a DataFrame with one column named “date” containing 61 rows (one for each day).

-----

#### Lines 21-26: Add Day Information with Transformations

```python
result = dates.withColumn("day_name", date_format(col("date"), "EEEE")) \
              .withColumn("day_of_week", dayofweek(col("date"))) \
              .withColumn("day_type", 
                         when(col("day_of_week").isin(1, 7), "Weekend")
                         .otherwise("Weekday")) \
              .orderBy("date")
```

**Understanding the Structure:**

- Chain of method calls (method chaining)
- Each `.withColumn()` adds a new column
- `\` at end of line: Python line continuation character
- Each transformation returns a **new** DataFrame (DataFrames are immutable)

-----

**Line 21: Add day_name Column**

```python
result = dates.withColumn("day_name", date_format(col("date"), "EEEE"))
```

**`dates`**

- The starting DataFrame (contains only “date” column)

**`.withColumn(column_name, column_expression)`**

- Method to add a new column or replace an existing one
- Returns a **new** DataFrame (doesn’t modify original)
- Two parameters:
1. `column_name`: Name of column to create/replace
1. `column_expression`: How to calculate the column value

**`"day_name"`**

- Name of the new column we’re creating

**`date_format(col("date"), "EEEE")`**

- Expression to calculate the column value
- `col("date")`: References the “date” column in the DataFrame
- `date_format(...)`: PySpark function to format dates as strings
- `"EEEE"`: Format pattern for full day name

**Format pattern options:**

- `"EEEE"` → `"Wednesday"` (full day name)
- `"EEE"` → `"Wed"` (abbreviated, 3 letters)
- `"E"` → `"Wed"` (short form)

**What happens:**

```
Input DataFrame:
┌────────────┐
│    date    │
├────────────┤
│ 2026-01-16 │
│ 2026-01-17 │
│ 2026-01-18 │
└────────────┘

After .withColumn("day_name", ...):
┌────────────┬───────────┐
│    date    │ day_name  │
├────────────┼───────────┤
│ 2026-01-16 │ Friday    │
│ 2026-01-17 │ Saturday  │
│ 2026-01-18 │ Sunday    │
└────────────┴───────────┘
```

**Result:** DataFrame now has 2 columns: `[date, day_name]`

-----

**Line 22: Add day_of_week Column**

```python
.withColumn("day_of_week", dayofweek(col("date")))
```

**`.withColumn("day_of_week", ...)`**

- Add another column named “day_of_week”

**`dayofweek(col("date"))`**

- `dayofweek()`: PySpark function to extract day of week as integer
- `col("date")`: References the date column
- Returns: **1-7** (where 1 = Sunday, 7 = Saturday)

**Important difference from PostgreSQL:**

- PostgreSQL `EXTRACT(DOW)`: 0-6 (0 = Sunday)
- PySpark `dayofweek()`: 1-7 (1 = Sunday)

**Mapping:**

```
Day       | PostgreSQL | PySpark
----------|------------|--------
Sunday    | 0          | 1
Monday    | 1          | 2
Tuesday   | 2          | 3
Wednesday | 3          | 4
Thursday  | 4          | 5
Friday    | 5          | 6
Saturday  | 6          | 7
```

**What happens:**

```
After .withColumn("day_of_week", ...):
┌────────────┬───────────┬─────────────┐
│    date    │ day_name  │ day_of_week │
├────────────┼───────────┼─────────────┤
│ 2026-01-16 │ Friday    │ 6           │
│ 2026-01-17 │ Saturday  │ 7           │
│ 2026-01-18 │ Sunday    │ 1           │
└────────────┴───────────┴─────────────┘
```

**Result:** DataFrame now has 3 columns: `[date, day_name, day_of_week]`

-----

**Lines 23-25: Add day_type Column (Conditional Logic)**

```python
.withColumn("day_type", 
           when(col("day_of_week").isin(1, 7), "Weekend")
           .otherwise("Weekday"))
```

**`.withColumn("day_type", ...)`**

- Add another column named “day_type”

Now let’s break down the conditional expression (the complex part):

**`when(condition, value)`** - Start Conditional

- PySpark function for conditional logic (like SQL CASE WHEN)
- First parameter: condition to check (boolean expression)
- Second parameter: value to return if condition is True
- Returns a `Column` object that can be chained

**`col("day_of_week")`**

- References the “day_of_week” column we just created
- This is a Column object

**`.isin(1, 7)`**

- Method on Column objects to check membership
- `isin()`: “is in” - checks if value is in the provided list
- `1, 7`: Sunday and Saturday (in PySpark’s 1-7 system)
- Returns Boolean: True if day_of_week is 1 or 7, False otherwise

**`"Weekend"`**

- Value to return if condition is True
- String literal

**`.otherwise("Weekday")`**

- Method chained after `when()`
- The “else” clause - what to return if condition is False
- `"Weekday"`: Value for Monday-Friday (days 2-6)

**Complete Logic:**

```python
when(col("day_of_week").isin(1, 7), "Weekend").otherwise("Weekday")

Equivalent to SQL:
CASE 
    WHEN day_of_week IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
END

Or Python if-else:
if day_of_week in [1, 7]:
    return "Weekend"
else:
    return "Weekday"
```

**Truth table:**

```
day_of_week | .isin(1, 7) | Result
------------|-------------|--------
1 (Sun)     | True        | Weekend
2 (Mon)     | False       | Weekday
3 (Tue)     | False       | Weekday
4 (Wed)     | False       | Weekday
5 (Thu)     | False       | Weekday
6 (Fri)     | False       | Weekday
7 (Sat)     | True        | Weekend
```

**What happens:**

```
After .withColumn("day_type", ...):
┌────────────┬───────────┬─────────────┬──────────┐
│    date    │ day_name  │ day_of_week │ day_type │
├────────────┼───────────┼─────────────┼──────────┤
│ 2026-01-16 │ Friday    │ 6           │ Weekday  │
│ 2026-01-17 │ Saturday  │ 7           │ Weekend  │
│ 2026-01-18 │ Sunday    │ 1           │ Weekend  │
│ 2026-01-19 │ Monday    │ 2           │ Weekday  │
└────────────┴───────────┴─────────────┴──────────┘
```

**Result:** DataFrame now has 4 columns: `[date, day_name, day_of_week, day_type]`

-----

**Line 26: Sort Results**

```python
.orderBy("date")
```

**`.orderBy(column_name)`**

- Method to sort DataFrame rows
- `"date"`: Sort by the date column
- Default is **ascending** order (earliest date first)
- Returns a new sorted DataFrame

**Alternative syntaxes:**

```python
# All equivalent:
.orderBy("date")                    # String
.orderBy(col("date"))              # Column object
.sort("date")                       # Alias for orderBy

# Descending order:
.orderBy(col("date").desc())       # Latest first
```

**What happens:**

```
Before orderBy: (already in order from sequence generation)
After orderBy: Explicitly sorted by date (ascending)
```

**Result:** Final DataFrame sorted by date, earliest to latest

-----

#### Line 29: Display Results

```python
result.show(20, truncate=False)
```

**`result`**

- The final DataFrame we created with all transformations

**`.show(n, truncate)`**

- Method to display DataFrame contents in console
- Prints rows in table format
- Useful for debugging and quick inspection

**Parameters:**

**`20`** - First parameter: number of rows

- Shows the first 20 rows
- Default is 20 if not specified
- Use `.show(100)` for 100 rows, etc.

**`truncate=False`** - Second parameter: truncation

- `False`: Show full column values (don’t cut off)
- `True` (default): Truncate values longer than 20 characters
- Can also be integer: `truncate=10` (truncate at 10 characters)

**Examples:**

```python
# Show first 20 rows, truncate long values
result.show()

# Show first 10 rows, no truncation
result.show(10, truncate=False)

# Show all rows (dangerous for large datasets!)
result.show(result.count(), truncate=False)
```

**Output format:**

```
+----------+---------+------------+--------+
|date      |day_name |day_of_week |day_type|
+----------+---------+------------+--------+
|2026-01-16|Friday   |6           |Weekday |
|2026-01-17|Saturday |7           |Weekend |
|2026-01-18|Sunday   |1           |Weekend |
...
+----------+---------+------------+--------+
```

**Note:** `.show()` is an **action** (triggers computation). Transformations like `.withColumn()` are **lazy** (don’t execute until an action is called).

-----

### Execution Flow

Let’s trace through the complete execution with actual data:

```
┌─────────────────────────────────────────────────────┐
│ STEP 1: Calculate Dates (Python)                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Current Date: 2026-02-15 (from datetime.now())      │
│                                                     │
│ start_date = 2026-02-15 - 30 days                   │
│            = 2026-01-16                             │
│                                                     │
│ end_date = 2026-02-15 + 30 days                     │
│          = 2026-03-17                               │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 2: Generate Date Sequence (Spark SQL)          │
├─────────────────────────────────────────────────────┤
│                                                     │
│ spark.sql(f"SELECT explode(sequence(...)) AS date") │
│                                                     │
│ sequence(2026-01-16, 2026-03-17, 1 day):            │
│   → Creates array with 61 dates                     │
│   → [2026-01-16, 2026-01-17, ..., 2026-03-17]       │
│                                                     │
│ explode(array):                                     │
│   → Converts array to 61 separate rows              │
│                                                     │
│ Result: DataFrame with 1 column, 61 rows            │
│ ┌────────────┐                                      │
│ │    date    │                                      │
│ ├────────────┤                                      │
│ │ 2026-01-16 │                                      │
│ │ 2026-01-17 │                                      │
│ │    ...     │                                      │
│ └────────────┘                                      │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 3: Add day_name Column                         │
├─────────────────────────────────────────────────────┤
│                                                     │
│ .withColumn("day_name", date_format(..., "EEEE"))   │
│                                                     │
│ For each date, format as full day name:             │
│   2026-01-16 → "Friday"                             │
│   2026-01-17 → "Saturday"                           │
│   2026-01-18 → "Sunday"                             │
│                                                     │
│ Result: DataFrame with 2 columns, 61 rows           │
│ ┌────────────┬───────────┐                          │
│ │    date    │ day_name  │                          │
│ ├────────────┼───────────┤                          │
│ │ 2026-01-16 │ Friday    │                          │
│ │ 2026-01-17 │ Saturday  │                          │
│ │    ...     │    ...    │                          │
│ └────────────┴───────────┘                          │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 4: Add day_of_week Column                      │
├─────────────────────────────────────────────────────┤
│                                                     │
│ .withColumn("day_of_week", dayofweek(col("date")))  │
│                                                     │
│ Extract day of week as number (1-7):                │
│   2026-01-16 (Friday) → 6                           │
│   2026-01-17 (Saturday) → 7                         │
│   2026-01-18 (Sunday) → 1                           │
│                                                     │
│ Result: DataFrame with 3 columns, 61 rows           │
│ ┌────────────┬───────────┬─────────────┐            │
│ │    date    │ day_name  │ day_of_week │            │
│ ├────────────┼───────────┼─────────────┤            │
│ │ 2026-01-16 │ Friday    │ 6           │            │
│ │ 2026-01-17 │ Saturday  │ 7           │            │
│ │    ...     │    ...    │    ...      │            │
│ └────────────┴───────────┴─────────────┘            │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 5: Add day_type Column (Conditional)           │
├─────────────────────────────────────────────────────┤
│                                                     │
│ .withColumn("day_type",                             │
│   when(col("day_of_week").isin(1, 7), "Weekend")   │
│   .otherwise("Weekday"))                            │
│                                                     │
│ Classify based on day_of_week:                      │
│   6 (Friday) → in [1,7]? NO → "Weekday"            │
│   7 (Saturday) → in [1,7]? YES → "Weekend"         │
│   1 (Sunday) → in [1,7]? YES → "Weekend"           │
│   2 (Monday) → in [1,7]? NO → "Weekday"            │
│                                                     │
│ Result: DataFrame with 4 columns, 61 rows           │
│ ┌────────────┬───────────┬─────────────┬──────────┐ │
│ │    date    │ day_name  │ day_of_week │ day_type │ │
│ ├────────────┼───────────┼─────────────┼──────────┤ │
│ │ 2026-01-16 │ Friday    │ 6           │ Weekday  │ │
│ │ 2026-01-17 │ Saturday  │ 7           │ Weekend  │ │
│ │    ...     │    ...    │    ...      │   ...    │ │
│ └────────────┴───────────┴─────────────┴──────────┘ │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 6: Sort by Date                                │
├─────────────────────────────────────────────────────┤
│                                                     │
│ .orderBy("date")                                    │
│                                                     │
│ (Already in order from sequence, but explicitly     │
│  sorted for clarity)                                │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ STEP 7: Display Results                             │
├─────────────────────────────────────────────────────┤
│                                                     │
│ result.show(20, truncate=False)                     │
│                                                     │
│ Output (First 20 rows):                             │
│ +----------+---------+------------+--------+        │
│ |date      |day_name |day_of_week |day_type|        │
│ +----------+---------+------------+--------+        │
│ |2026-01-16|Friday   |6           |Weekday |        │
│ |2026-01-17|Saturday |7           |Weekend |        │
│ |2026-01-18|Sunday   |1           |Weekend |        │
│ |2026-01-19|Monday   |2           |Weekday |        │
│ |2026-01-20|Tuesday  |3           |Weekday |        │
│ |2026-01-21|Wednesday|4           |Weekday |        │
│ |2026-01-22|Thursday |5           |Weekday |        │
│ |2026-01-23|Friday   |6           |Weekday |        │
│ |2026-01-24|Saturday |7           |Weekend |        │
│ |2026-01-25|Sunday   |1           |Weekend |        │
│ |...       |...      |...         |...     |        │
│ +----------+---------+------------+--------+        │
└─────────────────────────────────────────────────────┘
```

-----

## What If Current Date Is Not Friday?

Great question! The solution works **regardless of what day today is** - it’s completely dynamic! Let me show you with different examples:

-----

### Example 1: Current Date = Monday (January 19, 2026)

#### Calculation

```
Current Date: 2026-01-19 (Monday)

Start Date = 2026-01-19 - 30 days = 2025-12-20 (Saturday)
End Date   = 2026-01-19 + 30 days = 2026-02-18 (Wednesday)

Total days: 61 days
```

#### Visual Timeline

```
    DECEMBER 2025       JANUARY 2026        FEBRUARY 2026
         |                   |                    |
        20 ← ← ← ← ← ← ← ← 19 → → → → → → → → 18
         ↑                   ↑                    ↑
      START               TODAY                 END
    (Saturday)           (Monday)           (Wednesday)
```

#### Calendar View

```
            DECEMBER 2025
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 1    2    3    4    5    6    7
 8    9   10   11   12   13   14
15   16   17   18   19  [20] [21]  ← START (Saturday)
[22] [23] [24] [25] [26] [27] [28]
[29] [30] [31]


            JANUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                     [1]  [2]  [3]
 [4]  [5]  [6]  [7]  [8]  [9] [10]
[11] [12] [13] [14] [15] [16] [17]
[18]《19》 [20] [21] [22] [23] [24]  ← TODAY (Monday)
[25] [26] [27] [28] [29] [30] [31]


           FEBRUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 [1]  [2]  [3]  [4]  [5]  [6]  [7]
 [8]  [9] [10] [11] [12] [13] [14]
[15] [16] [17] [18]  19   20   21  ← END (Wednesday)
 22   23   24   25   26   27   28
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2025-12-20 | Saturday   | 6           | Weekend  ← START
2025-12-21 | Sunday     | 0           | Weekend
2025-12-22 | Monday     | 1           | Weekday
2025-12-23 | Tuesday    | 2           | Weekday
...
2026-01-19 | Monday     | 1           | Weekday  ← TODAY
...
2026-02-16 | Monday     | 1           | Weekday
2026-02-17 | Tuesday    | 2           | Weekday
2026-02-18 | Wednesday  | 3           | Weekday  ← END
```

-----

### Example 2: Current Date = Wednesday (March 5, 2026)

#### Calculation

```
Current Date: 2026-03-05 (Wednesday)

Start Date = 2026-03-05 - 30 days = 2026-02-03 (Tuesday)
End Date   = 2026-03-05 + 30 days = 2026-04-04 (Saturday)

Total days: 61 days
```

#### Visual Timeline

```
    FEBRUARY 2026       MARCH 2026          APRIL 2026
         |                   |                    |
         3 ← ← ← ← ← ← ← ← 5 → → → → → → → → → 4
         ↑                   ↑                    ↑
      START               TODAY                 END
    (Tuesday)          (Wednesday)           (Saturday)
```

#### Calendar View

```
           FEBRUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                             [1]
 [2] [3]  [4]  [5]  [6]  [7]  [8]  ← START (Tuesday)
 [9] [10] [11] [12] [13] [14] [15]
[16] [17] [18] [19] [20] [21] [22]
[23] [24] [25] [26] [27] [28]


            MARCH 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 [1]  [2]  [3]  [4] [5]  [6]  [7]
                    《5》          ← TODAY (Wednesday)
 [8]  [9] [10] [11] [12] [13] [14]
[15] [16] [17] [18] [19] [20] [21]
[22] [23] [24] [25] [26] [27] [28]
[29] [30] [31]


            APRIL 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                 [1]  [2]  [3] [4]  ← END (Saturday)
  5   6    7    8    9   10   11
 12  13   14   15   16   17   18
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2026-02-03 | Tuesday    | 2           | Weekday  ← START
2026-02-04 | Wednesday  | 3           | Weekday
2026-02-05 | Thursday   | 4           | Weekday
2026-02-06 | Friday     | 5           | Weekday
2026-02-07 | Saturday   | 6           | Weekend
2026-02-08 | Sunday     | 0           | Weekend
...
2026-03-05 | Wednesday  | 3           | Weekday  ← TODAY
...
2026-04-02 | Thursday   | 4           | Weekday
2026-04-03 | Friday     | 5           | Weekday
2026-04-04 | Saturday   | 6           | Weekend  ← END
```

-----

### Example 3: Current Date = Sunday (June 28, 2026)

#### Calculation

```
Current Date: 2026-06-28 (Sunday)

Start Date = 2026-06-28 - 30 days = 2026-05-29 (Friday)
End Date   = 2026-06-28 + 30 days = 2026-07-28 (Tuesday)

Total days: 61 days
```

#### Visual Timeline

```
      MAY 2026            JUNE 2026           JULY 2026
         |                   |                    |
        29 ← ← ← ← ← ← ← ← 28 → → → → → → → → 28
         ↑                   ↑                    ↑
      START               TODAY                 END
     (Friday)            (Sunday)            (Tuesday)
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2026-05-29 | Friday     | 5           | Weekday  ← START
2026-05-30 | Saturday   | 6           | Weekend
2026-05-31 | Sunday     | 0           | Weekend
2026-06-01 | Monday     | 1           | Weekday
...
2026-06-27 | Saturday   | 6           | Weekend
2026-06-28 | Sunday     | 0           | Weekend  ← TODAY
2026-06-29 | Monday     | 1           | Weekday
...
2026-07-26 | Sunday     | 0           | Weekend
2026-07-27 | Monday     | 1           | Weekday
2026-07-28 | Tuesday    | 2           | Weekday  ← END
```

-----

### Why It Always Works

#### The Key: Dynamic Date Functions

**SQL:**

```sql
CURRENT_DATE  -- Always returns today's date, whatever day it is
```

**PySpark:**

```python
datetime.now().date()  -- Always returns today's date, whatever day it is
```

#### The Formula is Universal

```
Start Date = TODAY - 30 days  (Always works)
End Date   = TODAY + 30 days  (Always works)

Doesn't matter if TODAY is:
- Monday
- Tuesday  
- Wednesday
- Thursday
- Friday
- Saturday
- Sunday
```

-----

### Complete Flow for Any Day

Let’s say today is **Thursday, August 13, 2026**:

```
Step 1: Get current date
   CURRENT_DATE = 2026-08-13 (Thursday)

Step 2: Calculate start date
   2026-08-13 - 30 days = 2026-07-14 (Tuesday)

Step 3: Calculate end date  
   2026-08-13 + 30 days = 2026-09-12 (Saturday)

Step 4: Generate all dates from 2026-07-14 to 2026-09-12
   2026-07-14 (Tuesday)
   2026-07-15 (Wednesday)
   2026-07-16 (Thursday)
   ...
   2026-08-13 (Thursday) ← TODAY
   ...
   2026-09-12 (Saturday)

Step 5: Classify each date
   Tuesday → Weekday
   Wednesday → Weekday
   Thursday → Weekday
   Friday → Weekday
   Saturday → Weekend
   Sunday → Weekend
   Monday → Weekday
```

-----

### Testing with Different Days

#### SQL Query (Works Any Day)

```sql
-- Run this on ANY day and it will work!
WITH RECURSIVE DateSeries AS (
    SELECT CURRENT_DATE - INTERVAL '30 days' AS date  -- Dynamic!
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'    -- Dynamic!
)
SELECT 
    date,
    TO_CHAR(date, 'Day') AS day_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM DateSeries
ORDER BY date;
```

#### PySpark Code (Works Any Day)

```python
# This will work regardless of when you run it!
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek, when
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("AnyDay").getOrCreate()

# These calculations work for ANY current date
start_date = datetime.now().date() - timedelta(days=30)  # Dynamic!
end_date = datetime.now().date() + timedelta(days=30)    # Dynamic!

print(f"Today is: {datetime.now().date()}")
print(f"Today is: {datetime.now().strftime('%A')}")  # Day name
print(f"Start date: {start_date} ({start_date.strftime('%A')})")
print(f"End date: {end_date} ({end_date.strftime('%A')})")

dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) AS date
""")

result = dates.withColumn("day_name", date_format(col("date"), "EEEE")) \
              .withColumn("day_of_week", dayofweek(col("date"))) \
              .withColumn("day_type", 
                         when(col("day_of_week").isin(1, 7), "Weekend")
                         .otherwise("Weekday")) \
              .orderBy("date")

result.show(20, truncate=False)
```

-----

### Proof: Run on Different Days

#### Run on Monday

```python
# If you run this on Monday, Jan 19, 2026:
Today is: 2026-01-19
Today is: Monday
Start date: 2025-12-20 (Saturday)
End date: 2026-02-18 (Wednesday)
```

#### Run on Wednesday

```python
# If you run this on Wednesday, Mar 5, 2026:
Today is: 2026-03-05
Today is: Wednesday
Start date: 2026-02-03 (Tuesday)
End date: 2026-04-04 (Saturday)
```

#### Run on Sunday

```python
# If you run this on Sunday, Jun 28, 2026:
Today is: 2026-06-28
Today is: Sunday
Start date: 2026-05-29 (Friday)
End date: 2026-07-28 (Tuesday)
```

#### Run on Friday

```python
# If you run this on Friday, Sep 11, 2026:
Today is: 2026-09-11
Today is: Friday
Start date: 2026-08-12 (Wednesday)
End date: 2026-10-11 (Sunday)
```

-----

### Key Takeaways

#### The Magic: Dynamic Dates

✅ **No Hardcoding:** The solution uses `CURRENT_DATE` (SQL) or `datetime.now().date()` (Python)

✅ **Always Current:** Automatically adjusts based on when you run it

✅ **Works Any Day:** Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday - doesn’t matter!

✅ **Always 61 Days:** Always generates exactly 61 days (30 before + today + 30 after)

✅ **Correct Day Classification:** Automatically identifies weekdays and weekends regardless of starting day

#### Key Principle

```
The solution is DATE-AGNOSTIC!

It doesn't care what day today is.
It automatically:
1. Gets today's date
2. Calculates start (30 days before)
3. Calculates end (30 days after)
4. Generates all dates in between
5. Classifies each as Weekday or Weekend

Works on Monday? ✅
Works on Friday? ✅
Works on Sunday? ✅
Works on ANY day? ✅
```

-----

## Understanding Date Ranges

### Why -30 and +30 Days?

The `-30` and `+30` create a **window of dates around today**:

```
                    TODAY
                      ↓
    ←─────────────────┼─────────────────→
   -30 days         NOW          +30 days
   (PAST)                        (FUTURE)

Start Date         Current        End Date
2026-01-16        2026-02-15     2026-03-17
```

**Timeline View:**

```
December 2024        January 2025         February 2025
    |                    |                     |
    30 ← ← ← ← ← ← ← ← 15 → → → → → → → → → 17
    ↑                    ↑                     ↑
 START                TODAY                  END
(30 days ago)      (Right now)         (30 days ahead)
```

**Total range: 61 days**

- 30 days in the past
- Today (1 day)
- 30 days in the future

-----

### The Purpose of Date Windows

#### Core Purpose

Creating a date window allows you to **generate a continuous series of dates dynamically** for analysis, reporting, and data processing - even when your data has gaps or missing dates.

#### Problem Without Date Windows

**Example: Sales Data**

Without date window:

```
order_date | revenue
-----------|--------
2026-01-16 | 12,500
2026-01-18 | 8,900    ← Missing Jan 17!
2026-01-19 | 15,200
           ← Missing Jan 20-21 (weekend)
```

**Problems:**

- ❌ Missing dates don’t appear
- ❌ Can’t see zero-sales days
- ❌ Hard to calculate running totals
- ❌ Graphs have gaps
- ❌ Can’t identify trends properly

#### Solution With Date Windows

```
order_date | revenue
-----------|--------
2026-01-16 | 12,500
2026-01-17 | 0        ← Now visible!
2026-01-18 | 8,900
2026-01-19 | 15,200
2026-01-20 | 0        ← Now visible!
2026-01-21 | 0        ← Now visible!
```

**Benefits:**

- ✅ Complete timeline (no gaps)
- ✅ Shows zero-sales days
- ✅ Enables proper trend analysis
- ✅ Charts display correctly
- ✅ Running totals work properly

-----

### Customizing Date Ranges

The `-30` and `+30` are **arbitrary examples** - you can use any numbers!

#### Only Past Dates (Historical)

```sql
-- PostgreSQL: Last 30 days only
SELECT ...
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
  AND date <= CURRENT_DATE
```

```python
# PySpark: Last 30 days only
start_date = datetime.now().date() - timedelta(days=30)
end_date = datetime.now().date()
```

**Timeline:**

```
December          January
    30 ← ← ← ← ← 15
    ↑              ↑
  START          TODAY (END)
```

-----

#### Only Future Dates (Upcoming)

```sql
-- PostgreSQL: Next 30 days only
SELECT ...
WHERE date >= CURRENT_DATE
  AND date <= CURRENT_DATE + INTERVAL '30 days'
```

```python
# PySpark: Next 30 days only
start_date = datetime.now().date()
end_date = datetime.now().date() + timedelta(days=30)
```

**Timeline:**

```
January           February
   15 → → → → → → 14
   ↑               ↑
TODAY (START)     END
```

-----

#### Entire Year

```sql
-- PostgreSQL: All of 2026
SELECT ...
WHERE date >= DATE '2026-01-01'
  AND date <= DATE '2026-12-31'
```

```python
# PySpark: All of 2026
start_date = datetime(2026, 1, 1).date()
end_date = datetime(2026, 12, 31).date()
```

**Timeline:**

```
January                              December
  01 → → → → → → → → → → → → → → → → 31
  ↑                                    ↑
START                                 END
```

-----

#### Custom Examples

```python
# Last week
days_back = 7
days_forward = 0

# Only next month
days_back = 0
days_forward = 30

# Entire quarter around today
days_back = 45
days_forward = 45

# Just today
days_back = 0
days_forward = 0
```

-----

## What If Current Date Is Not Friday?

**Important:** The solution works **regardless of what day today is** - it’s completely dynamic! The Friday examples used throughout this guide are just for demonstration. Let me prove it works for any day:

-----

### Example 1: Current Date = Monday (January 19, 2026)

#### Calculation

```
Current Date: 2026-01-19 (Monday)

Start Date = 2026-01-19 - 30 days = 2025-12-20 (Saturday)
End Date   = 2026-01-19 + 30 days = 2026-02-18 (Wednesday)

Total days: 61 days
```

#### Visual Timeline

```
    DECEMBER 2025       JANUARY 2026        FEBRUARY 2026
         |                   |                    |
        20 ← ← ← ← ← ← ← ← 19 → → → → → → → → 18
         ↑                   ↑                    ↑
      START               TODAY                 END
    (Saturday)           (Monday)           (Wednesday)
```

#### Calendar View

```
            DECEMBER 2025
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 1    2    3    4    5    6    7
 8    9   10   11   12   13   14
15   16   17   18   19  [20] [21]  ← START (Saturday)
[22] [23] [24] [25] [26] [27] [28]
[29] [30] [31]


            JANUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                     [1]  [2]  [3]
 [4]  [5]  [6]  [7]  [8]  [9] [10]
[11] [12] [13] [14] [15] [16] [17]
[18]《19》 [20] [21] [22] [23] [24]  ← TODAY (Monday)
[25] [26] [27] [28] [29] [30] [31]


           FEBRUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 [1]  [2]  [3]  [4]  [5]  [6]  [7]
 [8]  [9] [10] [11] [12] [13] [14]
[15] [16] [17] [18]  19   20   21  ← END (Wednesday)
 22   23   24   25   26   27   28
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2025-12-20 | Saturday   | 6           | Weekend  ← START
2025-12-21 | Sunday     | 0           | Weekend
2025-12-22 | Monday     | 1           | Weekday
2025-12-23 | Tuesday    | 2           | Weekday
...
2026-01-19 | Monday     | 1           | Weekday  ← TODAY
...
2026-02-16 | Monday     | 1           | Weekday
2026-02-17 | Tuesday    | 2           | Weekday
2026-02-18 | Wednesday  | 3           | Weekday  ← END
```

-----

### Example 2: Current Date = Wednesday (March 5, 2026)

#### Calculation

```
Current Date: 2026-03-05 (Wednesday)

Start Date = 2026-03-05 - 30 days = 2026-02-03 (Tuesday)
End Date   = 2026-03-05 + 30 days = 2026-04-04 (Saturday)

Total days: 61 days
```

#### Visual Timeline

```
    FEBRUARY 2026       MARCH 2026          APRIL 2026
         |                   |                    |
         3 ← ← ← ← ← ← ← ← 5 → → → → → → → → → 4
         ↑                   ↑                    ↑
      START               TODAY                 END
    (Tuesday)          (Wednesday)           (Saturday)
```

#### Calendar View

```
           FEBRUARY 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                             [1]
 [2] [3]  [4]  [5]  [6]  [7]  [8]  ← START (Tuesday)
 [9] [10] [11] [12] [13] [14] [15]
[16] [17] [18] [19] [20] [21] [22]
[23] [24] [25] [26] [27] [28]


            MARCH 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
 [1]  [2]  [3]  [4] [5]  [6]  [7]
                    《5》          ← TODAY (Wednesday)
 [8]  [9] [10] [11] [12] [13] [14]
[15] [16] [17] [18] [19] [20] [21]
[22] [23] [24] [25] [26] [27] [28]
[29] [30] [31]


            APRIL 2026
Mon  Tue  Wed  Thu  Fri  Sat  Sun
                 [1]  [2]  [3] [4]  ← END (Saturday)
  5   6    7    8    9   10   11
 12  13   14   15   16   17   18
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2026-02-03 | Tuesday    | 2           | Weekday  ← START
2026-02-04 | Wednesday  | 3           | Weekday
2026-02-05 | Thursday   | 4           | Weekday
2026-02-06 | Friday     | 5           | Weekday
2026-02-07 | Saturday   | 6           | Weekend
2026-02-08 | Sunday     | 0           | Weekend
...
2026-03-05 | Wednesday  | 3           | Weekday  ← TODAY
...
2026-04-02 | Thursday   | 4           | Weekday
2026-04-03 | Friday     | 5           | Weekday
2026-04-04 | Saturday   | 6           | Weekend  ← END
```

-----

### Example 3: Current Date = Sunday (June 28, 2026)

#### Calculation

```
Current Date: 2026-06-28 (Sunday)

Start Date = 2026-06-28 - 30 days = 2026-05-29 (Friday)
End Date   = 2026-06-28 + 30 days = 2026-07-28 (Tuesday)

Total days: 61 days
```

#### Visual Timeline

```
      MAY 2026            JUNE 2026           JULY 2026
         |                   |                    |
        29 ← ← ← ← ← ← ← ← 28 → → → → → → → → 28
         ↑                   ↑                    ↑
      START               TODAY                 END
     (Friday)            (Sunday)            (Tuesday)
```

#### Sample Output

```
date       | day_name   | day_of_week | day_type
-----------|------------|-------------|----------
2026-05-29 | Friday     | 5           | Weekday  ← START
2026-05-30 | Saturday   | 6           | Weekend
2026-05-31 | Sunday     | 0           | Weekend
2026-06-01 | Monday     | 1           | Weekday
...
2026-06-27 | Saturday   | 6           | Weekend
2026-06-28 | Sunday     | 0           | Weekend  ← TODAY
2026-06-29 | Monday     | 1           | Weekday
...
2026-07-26 | Sunday     | 0           | Weekend
2026-07-27 | Monday     | 1           | Weekday
2026-07-28 | Tuesday    | 2           | Weekday  ← END
```

-----

### Why It Always Works

#### The Key: Dynamic Date Functions

**SQL:**

```sql
CURRENT_DATE  -- Always returns today's date, whatever day it is
```

**PySpark:**

```python
datetime.now().date()  -- Always returns today's date, whatever day it is
```

#### The Formula is Universal

```
Start Date = TODAY - 30 days  (Always works)
End Date   = TODAY + 30 days  (Always works)

Doesn't matter if TODAY is:
- Monday    ✅
- Tuesday   ✅
- Wednesday ✅
- Thursday  ✅
- Friday    ✅
- Saturday  ✅
- Sunday    ✅
```

-----

### Complete Flow for Any Day

Let’s say today is **Thursday, August 13, 2026**:

```
Step 1: Get current date
   CURRENT_DATE = 2026-08-13 (Thursday)

Step 2: Calculate start date
   2026-08-13 - 30 days = 2026-07-14 (Tuesday)

Step 3: Calculate end date  
   2026-08-13 + 30 days = 2026-09-12 (Saturday)

Step 4: Generate all dates from 2026-07-14 to 2026-09-12
   2026-07-14 (Tuesday)
   2026-07-15 (Wednesday)
   2026-07-16 (Thursday)
   ...
   2026-08-13 (Thursday) ← TODAY
   ...
   2026-09-12 (Saturday)

Step 5: Classify each date
   Tuesday → Weekday
   Wednesday → Weekday
   Thursday → Weekday
   Friday → Weekday
   Saturday → Weekend
   Sunday → Weekend
   Monday → Weekday
```

-----

### Testing with Different Days

#### SQL Query (Works Any Day)

```sql
-- Run this on ANY day and it will work!
WITH RECURSIVE DateSeries AS (
    SELECT CURRENT_DATE - INTERVAL '30 days' AS date  -- Dynamic!
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'    -- Dynamic!
)
SELECT 
    date,
    TO_CHAR(date, 'Day') AS day_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM DateSeries
ORDER BY date;
```

#### PySpark Code (Works Any Day)

```python
# This will work regardless of when you run it!
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek, when
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("AnyDay").getOrCreate()

# These calculations work for ANY current date
start_date = datetime.now().date() - timedelta(days=30)  # Dynamic!
end_date = datetime.now().date() + timedelta(days=30)    # Dynamic!

print(f"Today is: {datetime.now().date()}")
print(f"Today is: {datetime.now().strftime('%A')}")  # Day name
print(f"Start date: {start_date} ({start_date.strftime('%A')})")
print(f"End date: {end_date} ({end_date.strftime('%A')})")

dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) AS date
""")

result = dates.withColumn("day_name", date_format(col("date"), "EEEE")) \
              .withColumn("day_of_week", dayofweek(col("date"))) \
              .withColumn("day_type", 
                         when(col("day_of_week").isin(1, 7), "Weekend")
                         .otherwise("Weekday")) \
              .orderBy("date")

result.show(20, truncate=False)
```

-----

### Proof: Run on Different Days

#### Run on Monday

```python
# If you run this on Monday, Jan 19, 2026:
Today is: 2026-01-19
Today is: Monday
Start date: 2025-12-20 (Saturday)
End date: 2026-02-18 (Wednesday)
```

#### Run on Wednesday

```python
# If you run this on Wednesday, Mar 5, 2026:
Today is: 2026-03-05
Today is: Wednesday
Start date: 2026-02-03 (Tuesday)
End date: 2026-04-04 (Saturday)
```

#### Run on Sunday

```python
# If you run this on Sunday, Jun 28, 2026:
Today is: 2026-06-28
Today is: Sunday
Start date: 2026-05-29 (Friday)
End date: 2026-07-28 (Tuesday)
```

#### Run on Friday

```python
# If you run this on Friday, Sep 11, 2026:
Today is: 2026-09-11
Today is: Friday
Start date: 2026-08-12 (Wednesday)
End date: 2026-10-11 (Sunday)
```

-----

### Summary: The Magic of Dynamic Dates

**Key Principle:**

```
The solution is DATE-AGNOSTIC!

It doesn't care what day today is.
It automatically:
1. Gets today's date
2. Calculates start (30 days before)
3. Calculates end (30 days after)
4. Generates all dates in between
5. Classifies each as Weekday or Weekend

Works on Monday?    ✅
Works on Friday?    ✅
Works on Sunday?    ✅
Works on ANY day?   ✅
```

**Why It Always Works:**

✅ **No Hardcoding:** Uses `CURRENT_DATE` (SQL) or `datetime.now().date()` (Python)

✅ **Always Current:** Automatically adjusts based on when you run it

✅ **Works Any Day:** Monday through Sunday - doesn’t matter!

✅ **Always 61 Days:** Always generates exactly 61 days (30 before + today + 30 after)

✅ **Correct Classification:** Automatically identifies weekdays and weekends regardless of starting day

-----

## Real-World Use Cases

### 1. Sales Reports with Zero-Revenue Days

**Business Need:** “Show me daily revenue for the last 30 days, including days with zero sales”

**SQL Implementation:**

```sql
WITH DateRange AS (
    SELECT generate_series(
        CURRENT_DATE - INTERVAL '30 days',
        CURRENT_DATE,
        INTERVAL '1 day'
    )::date AS date
)
SELECT 
    d.date,
    COALESCE(SUM(o.amount), 0) AS daily_revenue,
    COUNT(o.order_id) AS order_count
FROM DateRange d
LEFT JOIN orders o ON d.date::date = o.order_date::date
GROUP BY d.date
ORDER BY d.date;
```

**Why the window?**

- Shows **every single day** even if no sales occurred
- Helps identify patterns (slow days, trends, seasonality)
- Makes charts continuous (no missing data points)

**Output:**

```
date       | daily_revenue | order_count
-----------|---------------|------------
2026-01-16 | 12,500        | 45
2026-01-17 | 8,300         | 32
2026-01-18 | 0             | 0   ← Store closed
2026-01-19 | 15,600        | 56
2026-01-20 | 0             | 0   ← Weekend
2026-01-21 | 0             | 0   ← Weekend
```

-----

### 2. Employee Attendance Tracking

**Business Need:** “Show attendance for all employees for the entire month”

**SQL Implementation:**

```sql
WITH 
    AllDates AS (
        SELECT generate_series(
            '2026-02-01'::date,
            '2026-02-28'::date,
            INTERVAL '1 day'
        )::date AS date
    ),
    AllEmployees AS (
        SELECT DISTINCT employee_id, employee_name 
        FROM employees
    )
SELECT 
    e.employee_name,
    d.date,
    TO_CHAR(d.date, 'Day') AS day_name,
    CASE 
        WHEN a.check_in_time IS NOT NULL THEN 'Present'
        WHEN EXTRACT(DOW FROM d.date) IN (0, 6) THEN 'Weekend'
        ELSE 'Absent'
    END AS status
FROM AllEmployees e
CROSS JOIN AllDates d
LEFT JOIN attendance a 
    ON e.employee_id = a.employee_id 
    AND d.date = a.attendance_date
ORDER BY e.employee_name, d.date;
```

**Why the window?**

- Creates a **complete attendance grid**
- Shows absent days (not just present days)
- Identifies patterns (frequent absences)
- Calculates attendance percentage accurately

**Output:**

```
employee_name | date       | day_name  | status
--------------|------------|-----------|--------
Alice         | 2026-02-01 | Saturday  | Weekend
Alice         | 2026-02-02 | Sunday    | Weekend
Alice         | 2026-02-03 | Monday    | Absent   ← Important!
Alice         | 2026-02-04 | Tuesday   | Present
...
Bob           | 2026-02-01 | Saturday  | Weekend
Bob           | 2026-02-02 | Sunday    | Weekend
Bob           | 2026-02-03 | Monday    | Present
...
```

-----

### 3. Subscription & Churn Analysis

**Business Need:** “Show daily active subscribers over time”

**SQL Implementation:**

```sql
WITH DateSeries AS (
    SELECT generate_series(
        '2026-01-01'::date,
        '2026-12-31'::date,
        INTERVAL '1 day'
    )::date AS date
)
SELECT 
    d.date,
    COUNT(DISTINCT s.user_id) AS active_subscribers,
    SUM(CASE WHEN d.date = s.start_date THEN 1 ELSE 0 END) AS new_subscribers,
    SUM(CASE WHEN d.date = s.end_date THEN 1 ELSE 0 END) AS churned_subscribers
FROM DateSeries d
LEFT JOIN subscriptions s 
    ON d.date >= s.start_date 
    AND (d.date <= s.end_date OR s.end_date IS NULL)
GROUP BY d.date
ORDER BY d.date;
```

**Why the window?**

- Shows **daily active count** for every single day
- No gaps in subscriber timeline
- Enables churn rate calculation
- Identifies growth/decline trends

**Output:**

```
date       | active_subscribers | new_subscribers | churned_subscribers
-----------|-------------------|-----------------|--------------------
2026-01-01 | 1,245             | 5               | 2
2026-01-02 | 1,248             | 8               | 5
2026-01-03 | 1,251             | 10              | 7
2026-01-04 | 1,254             | 6               | 3
```

-----

### 4. Stock Market Analysis

**Business Need:** “Show daily stock prices including weekends (to see gaps)”

**SQL Implementation:**

```sql
WITH TradingDays AS (
    SELECT generate_series(
        CURRENT_DATE - INTERVAL '90 days',
        CURRENT_DATE,
        INTERVAL '1 day'
    )::date AS date
)
SELECT 
    d.date,
    TO_CHAR(d.date, 'Day') AS day_name,
    COALESCE(s.closing_price, 
             LAG(s.closing_price) OVER (ORDER BY d.date)) AS price,
    CASE 
        WHEN EXTRACT(DOW FROM d.date) IN (0, 6) THEN 'Market Closed'
        WHEN s.closing_price IS NULL THEN 'Holiday'
        ELSE 'Trading Day'
    END AS market_status
FROM TradingDays d
LEFT JOIN stock_prices s ON d.date = s.trade_date
ORDER BY d.date;
```

**Why the window?**

- Shows **all calendar days** (trading and non-trading)
- Highlights market closures (weekends, holidays)
- Enables gap analysis
- Helps calculate holding period returns

-----

### 5. Budget vs Actual Comparison

**Business Need:** “Compare planned budget to actual spending for each day”

**SQL Implementation:**

```sql
WITH MonthDays AS (
    SELECT generate_series(
        date_trunc('month', CURRENT_DATE),
        date_trunc('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day',
        INTERVAL '1 day'
    )::date AS date
)
SELECT 
    d.date,
    b.daily_budget,
    COALESCE(SUM(e.amount), 0) AS actual_spending,
    b.daily_budget - COALESCE(SUM(e.amount), 0) AS variance,
    CASE 
        WHEN COALESCE(SUM(e.amount), 0) > b.daily_budget THEN 'Over Budget'
        WHEN COALESCE(SUM(e.amount), 0) < b.daily_budget THEN 'Under Budget'
        ELSE 'On Budget'
    END AS status
FROM MonthDays d
LEFT JOIN budget b ON EXTRACT(DAY FROM d.date) = b.day_of_month
LEFT JOIN expenses e ON d.date = e.expense_date
GROUP BY d.date, b.daily_budget
ORDER BY d.date;
```

**Why the window?**

- Complete month view (every day)
- Shows days with no spending
- Tracks cumulative variance
- Helps with cash flow planning

-----

## Advanced Examples

### 1. Complete Calendar Generator (PySpark)

```python
from pyspark.sql.functions import (
    col, date_format, dayofweek, weekofyear,
    when, year, month, dayofmonth
)
from datetime import datetime

class CalendarGenerator:
    """Generate complete calendars with weekday/weekend classification"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def generate_date_range(self, start_date=None, end_date=None, 
                           days_back=30, days_forward=30):
        """Generate date range dynamically"""
        if start_date is None:
            start_date = f"current_date() - interval {days_back} days"
        else:
            start_date = f"to_date('{start_date}')"
        
        if end_date is None:
            end_date = f"current_date() + interval {days_forward} days"
        else:
            end_date = f"to_date('{end_date}')"
        
        query = f"""
            SELECT explode(sequence(
                {start_date},
                {end_date},
                interval 1 day
            )) AS date
        """
        return self.spark.sql(query)
    
    def add_calendar_info(self, dates_df):
        """Add comprehensive calendar information"""
        return dates_df \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", dayofmonth(col("date"))) \
            .withColumn("day_name", date_format(col("date"), "EEEE")) \
            .withColumn("day_of_week", dayofweek(col("date"))) \
            .withColumn("day_type",
                       when(col("day_of_week").isin(1, 7), "Weekend")
                       .otherwise("Weekday")) \
            .withColumn("week_number", weekofyear(col("date"))) \
            .withColumn("month_name", date_format(col("date"), "MMMM")) \
            .withColumn("formatted_date", date_format(col("date"), "yyyy-MM-dd"))
    
    def generate_full_year(self, year=None):
        """Generate calendar for entire year"""
        if year is None:
            year = datetime.now().year
        
        dates_df = self.generate_date_range(
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31"
        )
        return self.add_calendar_info(dates_df).orderBy("date")
    
    def get_weekdays_only(self, dates_df):
        """Filter to weekdays only"""
        return dates_df.filter(col("day_type") == "Weekday")
    
    def get_weekends_only(self, dates_df):
        """Filter to weekends only"""
        return dates_df.filter(col("day_type") == "Weekend")
    
    def summarize_calendar(self, dates_df):
        """Get summary statistics"""
        print("=== Calendar Summary ===")
        
        print("\nBy Day Type:")
        dates_df.groupBy("day_type").count().orderBy("day_type").show()
        
        print("\nBy Day Name:")
        dates_df.groupBy("day_name", "day_type") \
                .count() \
                .orderBy("day_type", "day_name") \
                .show()

# Usage
spark = SparkSession.builder.appName("Calendar").getOrCreate()
calendar = CalendarGenerator(spark)

# Generate full year
full_year = calendar.generate_full_year(2026)
full_year.show(50)

# Get only weekdays
weekdays = calendar.get_weekdays_only(full_year)
weekdays.show(20)

# Get summary
calendar.summarize_calendar(full_year)
```

-----

### 2. Add Hierarchy Level to Dates

```sql
WITH RECURSIVE DateSeries AS (
    SELECT 
        CURRENT_DATE - INTERVAL '30 days' AS date,
        0 AS level
    
    UNION ALL
    
    SELECT 
        date + INTERVAL '1 day',
        level + 1
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'
)
SELECT 
    date,
    level,
    TO_CHAR(date, 'Day') AS day_name,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM DateSeries
ORDER BY date;
```

**Output:**

```
date       | level | day_name  | day_type
-----------|-------|-----------|----------
2026-01-16 | 0     | Friday    | Weekday
2026-01-17 | 1     | Saturday  | Weekend
2026-01-18 | 2     | Sunday    | Weekend
2026-01-19 | 3     | Monday    | Weekday
```

-----

### 3. Generate Business Days Only (Exclude Weekends)

```sql
WITH RECURSIVE DateSeries AS (
    SELECT CURRENT_DATE - INTERVAL '30 days' AS date
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM DateSeries
    WHERE date < CURRENT_DATE + INTERVAL '30 days'
)
SELECT 
    date,
    TO_CHAR(date, 'Day') AS day_name
FROM DateSeries
WHERE EXTRACT(DOW FROM date) NOT IN (0, 6)  -- Exclude weekends
ORDER BY date;
```

**PySpark:**

```python
result = dates.withColumn("day_of_week", dayofweek(col("date"))) \
              .filter(~col("day_of_week").isin(1, 7)) \
              .select("date", date_format(col("date"), "EEEE").alias("day_name"))
```

-----

## Quick Reference

### SQL vs PySpark Comparison

|Aspect             |PostgreSQL                           |PySpark                                    |
|-------------------|-------------------------------------|-------------------------------------------|
|**Date Generation**|`WITH RECURSIVE` + `UNION ALL`       |`sequence()` + `explode()`                 |
|**Current Date**   |`CURRENT_DATE`                       |`current_date()` or `datetime.now().date()`|
|**Add Days**       |`date + INTERVAL '30 days'`          |`date + timedelta(days=30)`                |
|**Day of Week**    |`EXTRACT(DOW)` (0-6, 0=Sun)          |`dayofweek()` (1-7, 1=Sun)                 |
|**Format Date**    |`TO_CHAR(date, 'Day')`               |`date_format(col("date"), "EEEE")`         |
|**Conditional**    |`CASE WHEN ... THEN ... ELSE ... END`|`when(...).otherwise(...)`                 |
|**Sort**           |`ORDER BY date`                      |`.orderBy("date")`                         |

-----

### Common Date Ranges

|Requirement     |SQL                                                  |PySpark                                                                |
|----------------|-----------------------------------------------------|-----------------------------------------------------------------------|
|**Last 30 days**|`CURRENT_DATE - INTERVAL '30 days'` to `CURRENT_DATE`|`datetime.now().date() - timedelta(days=30)` to `datetime.now().date()`|
|**Next 30 days**|`CURRENT_DATE` to `CURRENT_DATE + INTERVAL '30 days'`|`datetime.now().date()` to `datetime.now().date() + timedelta(days=30)`|
|**This week**   |Start of week to end of week                         |Calculate based on day of week                                         |
|**This month**  |`date_trunc('month', CURRENT_DATE)` to end of month  |`datetime(year, month, 1)` to last day                                 |
|**This year**   |`date_trunc('year', CURRENT_DATE)` to end of year    |`datetime(year, 1, 1)` to `datetime(year, 12, 31)`                     |

-----

### Key Functions

#### PostgreSQL

```sql
-- Date functions
CURRENT_DATE                    -- Today's date
date + INTERVAL '1 day'         -- Add duration
date - INTERVAL '1 day'         -- Subtract duration
EXTRACT(DOW FROM date)          -- Day of week (0-6)
TO_CHAR(date, 'Day')            -- Format as string
generate_series(start, end, '1 day')  -- Generate dates

-- Conditional
CASE 
    WHEN condition THEN value
    ELSE other_value
END
```

#### PySpark

```python
# Date functions
from datetime import datetime, timedelta
from pyspark.sql.functions import (
    col, date_format, dayofweek, current_date,
    year, month, dayofmonth, weekofyear, when
)

# Generate dates
spark.sql("SELECT explode(sequence(start, end, interval 1 day)) AS date")

# Format and extract
date_format(col("date"), "EEEE")  # Day name
dayofweek(col("date"))            # Day of week (1-7)
year(col("date"))                 # Year
month(col("date"))                # Month

# Conditional
when(condition, value).otherwise(other_value)
```

-----

## Summary

### Key Takeaways

1. **Dynamic Date Generation**
- Use recursive CTEs in SQL
- Use `sequence()` + `explode()` in PySpark
- Never hardcode dates - use current date functions
1. **Date Windows Solve Real Problems**
- Fill gaps in data
- Enable complete timelines
- Support accurate analytics
- Make visualizations continuous
1. **Weekday/Weekend Classification**
- SQL: `EXTRACT(DOW)` returns 0-6 (0=Sunday)
- PySpark: `dayofweek()` returns 1-7 (1=Sunday)
- Use `CASE`/`when` for classification
1. **Customization is Easy**
- Change `-30` and `+30` to any numbers
- Adjust for different time periods
- Filter for business days only
- Add holidays or special days
1. **Best Practices**
- Always use dynamic dates (current date functions)
- Add comprehensive date information (day name, type, etc.)
- Consider time zones if working across regions
- Cache DataFrames in PySpark for reuse
- Test with different date ranges

-----

**Document Version:** 1.0  
**Last Updated:** January 2026  
**Focus:** Date Generation, Weekdays & Weekends  
**Platforms:** PostgreSQL, PySpark

-----
