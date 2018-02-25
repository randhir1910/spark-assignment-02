package model

case class customer(customer_id: Int, name: String, street: String, city: String, state: String, zip: Int)

case class sales(year: Int, month: Int, day: Int, customer_id: Int, price: Double)
