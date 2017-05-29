## Index Instacart Data into Elasticsearch 

For fun.

1. Clone the repo

2. `yarn` 

3. Dump Instacart data[1] into an SQLite database in `db/instacart`
  - Download the data from https://www.instacart.com/datasets/grocery-shopping-2017
  - Create a directory called `db` and extract the download there
  - `sqlite3 db/instacart`
  - ```
    .mode csv
    .import db/instacart_2017_05_01/aisles.csv aisles
    .import db/instacart_2017_05_01/departments.csv departments
    .import db/instacart_2017_05_01/order_products__train.csv order_products__train
    .import db/instacart_2017_05_01/orders.csv orders
    .import db/instacart_2017_05_01/products.csv products
    ```

4. `DEBUG=* node index.js http://user:password@localhost:9200`

> [1] “The Instacart Online Grocery Shopping Dataset 2017”, Accessed from
> https://www.instacart.com/datasets/grocery-shopping-2017 on Mon May 29
> 01:05:55 MST 2017
