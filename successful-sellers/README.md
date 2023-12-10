# SUCCESSFUL SELLERS PROJECT
## DESCRIPTION
We try to find which sellers are successful based on the metrics.<br>
Write a basic `API` that returns the successful and unsuccessful seller based on the parameter(s).
## INPUT DATA
- `Orders`: Order data of the sellers based on product breakdown.
- `Visit`: Visit data for each seller.
- `Score`: Score of the sellers.
- `Product Reviews`: Rate of each product based on product breakdown.
- `Seller Details`: Details of the seller.
## CALCULATION
If a seller satisfies `ALL` conditions below, it will be marked as `SUCCESSFUL` seller.
- `Condition 1`: Sellers who were created last year.
- `Condition 2`: Sellers who have total sold product count >= 4 
- `Condition 3`: Sellers who have total visits >= 3
- `Condition 4`: Sellers who have average product review >= 2.5 
- `Condition 5`: Sellers who have average seller score >= 5.0
## EXAMPLE RESULT
Keep the result in a table with `is_successful` column.
- Condition 1 Result (e.g 2023-01-01 00:00:00) 
- Condition 2 Result (e.g 3)
- Condition 3 Result (e.g 2)
- Condition 4 Result (e.g 3.53)
- Condition 5 Result (e.g 6.75) 
- is_successful (Boolean = true/false)