CREATE TABLE [dbo].[silver_payments] (

	[payment_id] varchar(8000) NULL, 
	[customer_id] varchar(8000) NULL, 
	[payment_date] date NULL, 
	[payment_method] varchar(8000) NULL, 
	[payment_status] varchar(8000) NULL, 
	[amount] float NULL
);