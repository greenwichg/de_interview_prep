- **Explain how you would debug a SQL query that suddenly became 3× slower in production.**
  - **Interviewers push for:**
    1. **Query plan analysis**
    2. **Join order + join type decisions**
    3. **Statistics & cardinality**
    4. **Materialization**
    5. **Predicate pushdown**
- **The orders table has skewed data causing slow group-by queries. How would you optimize it?**
  - **They dig into indexing strategy, statistics, distribution keys (Snowflake/Redshift), and file pruning.**