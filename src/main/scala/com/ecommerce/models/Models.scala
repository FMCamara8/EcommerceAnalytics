package com.ecommerce.models

// Une ligne de transactions.csv
case class Transaction(
                        transaction_id: String,
                        user_id: String,
                        product_id: String,
                        merchant_id: String,
                        amount: Option[Double],
                        timestamp: String,          // texte au format yyyyMMddHHmmss, parfois mal formé
                        location: String,
                        payment_method: String,
                        category: String
                      )

// Une ligne de users.json
case class User(
                 user_id: String,
                 age: Option[Int],
                 annual_income: Option[Double],
                 city: String,
                 customer_segment: String,
                 preferred_categories: Seq[String],  // une liste, par exemple ["Books", "Electronics"]
                 registration_date: String           // texte au format yyyyMMdd
               )

// Une ligne de products.parquet
case class Product(
                    product_id: String,
                    name: String,
                    category: String,
                    price: Option[Double],
                    merchant_id: String,
                    rating: Option[Double],
                    stock: Option[Int]
                  )

// Une ligne de merchants.csv
case class Merchant(
                     merchant_id: String,
                     name: String,
                     category: String,
                     region: String,
                     commission_rate: Option[Double],
                     establishment_date: String          // texte au format yyyyMMdd
                   )