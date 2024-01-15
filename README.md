# BQ Cost Optimization Assessment

## Set Up
- Navigate to colab.google and open a new notebook.
- Click GitHub on the left nav bar and search for: sam-pitcher/bq_cost_optimization_assessment
- Select big_query_optimization.ipynb
- Save a copy
- Ensure you clone the files noted in the Clone files from git section

## Summary
The purpose of this notebook is to summarize BigQuery pricing for your customer and Google to process SQL queries needed to monitor BigQuery cost and performance.

## BigQuery Editions
There are 2 pricing strategies for BigQuery
On-demand
Capacity

### On-demand
By default, queries are billed using the on-demand (per TiB) pricing model, where you pay for the data scanned by your queries.

With on-demand pricing, you will generally have access to up to 2,000 concurrent slots, shared among all queries in a single project. Periodically, BigQuery will temporarily burst beyond this limit to accelerate smaller queries. In addition, you might occasionally have fewer slots available if there is a high amount of contention for on-demand capacity in a specific location.

In Europe (eu), the price is $6.25 per TiB.

### Capacity
BigQuery offers a capacity-based analysis pricing model for customers who prefer a predictable cost for queries rather than paying the on-demand price per TiB of data processed.

There are 3 editions *pricing as of October 2023:

#### Standard Edition
| Commitment model | Hourly cost | Details |
| - | - | - |
| Pay as you go / autoscale | \$0.066 / slot hour | Billed per second with a 1 minute minimum |

#### Enterprise Edition
Commitment model | Hourly cost | Details |
| - | - | - |
| Pay as you go / autoscale | \$0.066 / slot hour | Billed per second with a 1 minute minimum |
| 1 yr commit | \$0.0528 / slot hour | Billed for 1 year |
| 3 yr commit | \$0.0396 / slot hour | Billed for 3 years |

#### Enterprise Plus Edition
Commitment model | Hourly cost | Details |
| - | - | - |
| Pay as you go / autoscale | \$0.11 / slot hour | Billed per second with a 1 minute minimum |
| 1 yr commit | \$0.0.88 / slot hour | Billed for 1 year |
| 3 yr commit | \$0.0.66 / slot hour | Billed for 3 years |

When deciding between standard and enterprise, it’s important to note there are a few main differences. In the table below, the key advantages of Enterprise are listed. We would advise customers to only use Standard edition for basic ad hoc querying, trials and test projects.



Feature | Standard | Enterprise |
| - | - | - |
| Compute model | Autoscaling | Autoscaling + Baseline |
| Maximum reservation size | 1600 slots | None |
| VPC Service Controls | None | VPC Service Controls Support |
| Data governance	| None | Column-level access control, Row-level security, Dynamic data masking |
| Business Intelligence acceleration | None | BI Engine |
| Materialized views | Can query existing Materialized Views | Create materialized views, Automatic refresh of materialized views, Manual refresh of materialized views, Direct query of materialized views, Smart tuning |
| Integrated machine learning | None | BiqQuery ML |
| Workload management | Users cannot set the maximum concurrency target | Advanced workload management (idle capacity sharing, target concurrency) |# bq_cost_optimization_assessment
