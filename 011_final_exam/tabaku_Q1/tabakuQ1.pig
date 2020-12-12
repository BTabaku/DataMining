/*

Using Pig Latin commands, perform the following operations:
1. Load files as 'investors' and 'stock_prices'

*/

investors = LOAD '/PIG_LATIN/input_files/investor.txt' AS(id:chararray, name:chararray,lastname:chararray, sharename:chararray,quantity:int);

stock_prices = LOAD '/PIG_LATIN/input_files/stock_prices.txt' AS(sharename:chararray, price:int);

/*
2. Display both files to make sure they are loaded correctly
*/

describe investors;
dump investors;

describe stock_prices;
dump stock_prices;

/*
3. Joins the two files ('investors' and 'stock_prices') by stock symbol
*/

joined_investor_stock_prices = JOIN investors BY sharename, stock_prices BY sharename;

/*
4. Display the joined file
*/

describe joined_investor_stock_prices;
dump joined_investor_stock_prices;

/*
5. Group the above file (joined file) by the 'lastname' of the investors and display the results
*/

group_joined_by_lastname = GROUP joined_investor_stock_prices by lastname;
dump group_joined_by_lastname;

/*
6. Calculate the total shares (simply the sum of shares among all stocks) and display results
*/

/*
group investos first 
*/

group_of_investors = GROUP investors ALL;
dump group_of_investors;

total_shares = FOREACH group_of_investors GENERATE group, SUM(investors.quantity);
dump total_shares;

/*
7. Calculate the total dollar amount that each investor has invested (shares per each stock multiplied by the stock price) and display the results
*/

investors_total_stock_price = FOREACH joined_investor_stock_prices GENERATE name, lastname, quantity * price as total_amount_price;

describe investors_total_stock_price;
dump investors_total_stock_price;

/*
group investors
*/

investors_group_by_name = GROUP investors_total_stock_price by (name, lastname);
describe investors_group_by_name;
dump investors_group_by_name;

/*
total amount of dollars
*/

total_dollar_amount = FOREACH investors_group_by_name GENERATE group, SUM(investors_total_stock_price.total_amount_price) as total_amount_of_dollars_spent;
describe total_dollar_amount;
dump total_dollar_amount;


/*
8. Filter the top two investors that have invested the most dollar amount and display the results
*/

filter_list = ORDER total_dollar_amount BY total_amount_of_dollars_spent DESC;
dump filter_list;

/*
listing top two
*/

top_investors = LIMIT filter_list 2;
dump top_investors;
