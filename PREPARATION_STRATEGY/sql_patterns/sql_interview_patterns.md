# SQL Interview Questions for Product-Based Companies

## 1. Top-Selling Product Per Category
**Question:** Write a query to identify the top-selling product per category using window functions.

**Expected Follow-ups:**
- ROW_NUMBER vs RANK vs DENSE_RANK
- Tie handling
- Partition strategy

---

## 2. Deduplication with Latest Record
**Question:** Given a transaction table with duplicates, write a dedup query that always keeps the latest record.

**Key Topics:**
- QUALIFY clause
- Window ordering
- Deterministic dedup logic

---

## 3. Rolling Averages at Scale
**Question:** How would you calculate 7-day and 30-day rolling averages for millions of rows efficiently?

**Discussion Points:**
- PARTITION usage
- Frame clauses
- Performance considerations

---

## 4. Consecutive Days Detection
**Question:** Write a query to detect users who placed orders on three consecutive days.

**Approach:**
- Classic PBC question
- Requires LAG function
- Date arithmetic
- Grouping logic

---

## 5. Revenue Contribution Without Subqueries
**Question:** Find the percentage contribution of each store to the company's total revenue without using subqueries.

**Expected Skill:**
- Mastery of window aggregates

---

## 6. Optimizing Skewed Data
**Question:** The orders table has skewed data causing slow group-by queries. How would you optimize it?

**Topics Covered:**
- Indexing strategy
- Statistics management
- Distribution keys (Snowflake/Redshift)
- File pruning

---

## 7. Dynamic Pivoting
**Question:** Write a query to pivot monthly revenue into columns dynamically.

**Follow-ups:**
- Static vs dynamic pivots
- Performance trade-offs

---

## 8. First-Time Purchasers
**Question:** Identify customers who purchased a product for the first time in a given month.

**Expected Solution:**
- Correct use of `MIN(order_date) OVER (PARTITION BY user)`
- Proper filtering logic

---

## 9. Session Identification from Clickstream
**Question:** Given a large clickstream table, find sessions for each user based on 30-minute inactivity windows.

**Note:** Walmart/Google commonly ask this question.

**Requirements:**
- LAG function
- Sessionization logic
- Timestamp handling

---

## 10. Query Performance Debugging
**Question:** Explain how you would debug a SQL query that suddenly became 3× slower in production.

**Interviewers Look For:**
- Query plan analysis
- Join order and join type decisions
- Statistics and cardinality estimation
- Materialization strategies
- Predicate pushdown optimization

---

## Note
These are actual SQL challenges asked in product-based company (PBC) interviews. Moving beyond LeetCode-style practice requires understanding these real-world scenarios.
