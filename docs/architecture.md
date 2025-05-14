# Architecture

Describe architecture here.
+-------------------+        +-------------+         +-------------+
| NYC Taxi Raw Data | -----> | S3 Raw Zone | <-----+ | Metadata &  |
|  (CSV or Parquet) |        +-------------+         | Config JSON |
+-------------------+                                  +-------------+
                                                           |
                                                           v
                                                    +--------------+
                                                    | AWS Glue Job |
                                                    | (Transform)  |
                                                    +--------------+
                                                           |
                 +-------------------+     +------------------------+
                 | External Lookup   | --> | Join + Filter in Glue  |
                 | (e.g. Zone Info)  |     +------------------------+
                                                           |
                                                           v
                                                  +----------------+
                                                  | S3 Clean Zone  |
                                                  +----------------+
                                                           |
                                                           v
                                                 +------------------+
                                                 | Load to Redshift |
                                                 +------------------+
                                                           |
                                                           v
                                               +------------------------+
                                               | Dashboard (Metabase)   |
                                               +------------------------+
