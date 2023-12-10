# CRITICAL THRESHOLD PROJECT
## DESCRIPTION
Try to find products that are at a critical threshold for each seller and product. Consider orders that are ordered at last N days(number of days defined below). So, sellers can take actions like adding new stocks for their products.
## INPUT DATA
- `Orders` Order data of sellers
- `Product` Product information
## VARIABLE EXPLANATIONS AND CALCULATION
- `Number of Days`Last 30 days to get related orders.
- `Stock`Represents how many items left in hand for a product.
- `Daily average sale quantity`The ratio of total sales count and number of days that a product is sold. Basically, total sale count / number of days
- `Daily average sale price`The ratio of total sales price and number of days that a product is sold. Basically, total sales / number of days
- `Critical Threshold Calculation`Stock / Daily Average Sale Quantity

Find products with a critical threshold under 30(N). The critical threshold might be changed.
## EXPECTED-EXAMPLE OUTPUT
EXPECTED OUTPUT:
>After finding products that are at critical condition, write an endpoint, and return product name, brand, color etc with a critical threshold. Return top 1 products desc by critical threshold for each seller and product. Return the result by basic API.

EXAMPLE OUTPUT:
<br><pre>{
 SellerId: 100,<br>
 productId: 1,<br> 
 dailyAverageSaleQuantity: 6,<br> 
 dailyAverageSalePrice: 300,<br> 
 criticalThreshold: 0.33, <br>
 productName: “helloworld”, <br>
 “brand”: “helloworld”, <br>
 “category”: “helloworld”, <br>
 “productSize”: “helloworld”<br>
}