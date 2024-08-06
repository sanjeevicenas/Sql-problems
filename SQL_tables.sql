-- Databricks notebook source
CREATE TABLE Country
(
 CountryId INTEGER CONSTRAINT pk_CountryId PRIMARY KEY,
 CountryName VARCHAR(30) NOT NULL
);

-- COMMAND ----------

INSERT INTO Country(CountryId,CountryName) VALUES(1,'India');
INSERT INTO Country(CountryId,CountryName) VALUES(2,'USA');
INSERT INTO Country(CountryId,CountryName) VALUES(3,'Brazil');
INSERT INTO Country(CountryId,CountryName) VALUES(4,'England');
INSERT INTO Country(CountryId,CountryName) VALUES(5,'Australia');
INSERT INTO Country(CountryId,CountryName) VALUES(6,'South Africa');

-- COMMAND ----------

CREATE TABLE CustomerDetail
(
 CustomerId INTEGER CONSTRAINT pk_CustomerId PRIMARY KEY,
 CustomerName VARCHAR(50) NOT NULL,
 Email VARCHAR(50) NOT NULL,
 ContactNumber VARCHAR(20) NOT NULL,
 Address VARCHAR(500) NOT NULL,
 CountryId INTEGER CONSTRAINT fk_CountryId_CustomerDetail REFERENCES Country(CountryId) NOT NULL
);

-- COMMAND ----------

INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES (1,'Albert','Albert@gmail.com','+91-8888888888','Mumbai, India',1);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(2,'Anabela','Anabela@gmail.com','+1-206-999-2222','Washinton DC, USA',2);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(3,'Angeles','Angie@gmail.com','+91-8888888888','Mumbai, India',1);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(4,'Carlos','Carlos@gmail.com','+55-21-4444-5555','Rio De Janeiro, Brazil',3);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(5,'Daniel','Daniel@gmail.com','+1-212-999-2222','New York, USA',2);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(6,'Davis','Davis@gmail.com','+55-21-4444-5555','Rio De Janeiro, Brazil',3);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(7,'Helen','Helen@gmail.com','+44-20-7777-3333','London, England',4);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(8,'Karin','Karin@gmail.com','+61-2-5555-2222','Canberra, Australia',5);
INSERT INTO CustomerDetail(CustomerId,CustomerName,Email,ContactNumber,Address,CountryId) 
VALUES(9,'Janine','Janine@gmail.com','+27-41-888-7777','Pretoria, South Africa',6);

-- COMMAND ----------



-- COMMAND ----------

CREATE TABLE SellerDetail
(
 SellerId INTEGER CONSTRAINT pk_SellerId PRIMARY KEY,
 SellerName VARCHAR(50) NOT NULL,
 SellerAddress VARCHAR(500) NOT NULL,
 SellerZip VARCHAR(15) NOT NULL,
 SellerContactNumber VARCHAR(20) NOT NULL,
 SellerRating DECIMAL(5),
 CountryId INTEGER CONSTRAINT fk_CountryId_SellerDetail REFERENCES Country(CountryId) NOT NULL
);

-- COMMAND ----------

alter table SellerDetail add constraint chk_SellerRating CHECK (SellerRating>=0 AND SellerRating<=5);

-- COMMAND ----------

INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(1,'WRetail','New Delhi, India', '100001','+91-9999999999',4.7,1);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip,SellerContactNumber, SellerRating, CountryId) 
VALUES(2,'DSMart','Washington, USA', '20009','+1-206-777-3333',4.2,2);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(3,'Vida','Rio De Janeiro, Brazil', '28640-000','+55-21-5555',3.8,3);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip,SellerContactNumber, SellerRating, CountryId) 
VALUES(4,'GBRRetail','London, England', 'WC2N','+44-20-7777-4444',3.3,4);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip,SellerContactNumber, SellerRating, CountryId) 
VALUES(5,'FoxRetail','Canberra, Australia', '2600','+61-2-5555-1111',2.9,5);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(6,'Makhaya Brothers','Pretoria, South Africa', '0001','+27-12-999-5555',2.5,6);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(7,'BangaloreRetail','Bangalore, India', '560063','+91-9999999998',4.3,1);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(8,'NYNew','New York, USA', '00501','+1-212-555-6666',3.8,2);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(9,'GarrinchaRetail','Brasilia, Brazil', '70000','+55-61-4444-3333',4.2,3);
INSERT INTO SellerDetail(SellerId, SellerName, SellerAddress, SellerZip, SellerContactNumber, SellerRating, CountryId) 
VALUES(10,'ABRetail','Port Elizabeth, South Africa', '6001','+27-41-888-6666',4.5,6);


-- COMMAND ----------

CREATE TABLE ProductInventory 
(
 ProductId INTEGER CONSTRAINT pk_ProductId PRIMARY KEY,
 ProductType VARCHAR(50) NOT NULL,
 ProductSubType VARCHAR(50) NOT NULL,
 ProductName VARCHAR(50) NOT NULL,
 Make VARCHAR(50) NOT NULL,
 Price DECIMAL(10,2) NOT NULL,
 SellerId INTEGER CONSTRAINT fk_SellerId_ProductInventory REFERENCES  SellerDetail(SellerId),
 WarrantyMonths INTEGER not null,
 UnitsLeft INTEGER  NOT NULL
);


-- COMMAND ----------

alter table ProductInventory add constraint chk_Price CHECK(Price>0);
alter table ProductInventory add constraint chk_WarrantyMonths CHECK(WarrantyMonths>=0);
alter table ProductInventory add constraint chk_UnitsLeft CHECK(UnitsLeft>=0);

-- COMMAND ----------

INSERT INTO ProductInventory VALUES(1,'Automobiles','Four wheeler','Lamborghini Gallardo Spyder','Lamborghini',18000000,1,60,2);
INSERT INTO ProductInventory VALUES(2,'Clothing and Accessory','Belt','Kenneth Cole Black and White Leather Belt','Kenneth Cole',2500,2,0,200);
INSERT INTO ProductInventory VALUES(3,'Automobiles','Two wheeler','Honda CBR 250R','Honda',193000,1,60,11);
INSERT INTO ProductInventory VALUES(4,'Electronics','Mobile','Apple IPhone 7 16GB','Apple',60000,1,24,20);
INSERT INTO ProductInventory VALUES(5,'Electronics','Mobile','Lumia 1320','Microsoft',42199,2,24,20);
INSERT INTO ProductInventory VALUES(6,'Home','Furniture','Wooden photo frame','AllWood',3,2,0,200);
INSERT INTO ProductInventory VALUES(7,'Jewellery','Women Jewellery','Kundan jewellery set','Kundan',2000,1,0,20);
INSERT INTO ProductInventory VALUES(8,'Shoes','Sports','Adidas Shoes','Adidas',700,3,0,10);
INSERT INTO ProductInventory VALUES(9,'Sports','Lawn Tennis','Tennis racket','LT',200,4,0,150);
INSERT INTO ProductInventory VALUES(10,'Health','Gym','Door gym','GD',700,5,0,20);
INSERT INTO ProductInventory VALUES(11,'Automobiles','Four wheeler','BMW Z4','BMW',6890000,6,0,20);

-- COMMAND ----------

CREATE TABLE BillingDetail
(
 BillId INTEGER,
 BillItemId INTEGER,
 CustomerId INTEGER CONSTRAINT fk_CustomerId_BillingDetail REFERENCES CustomerDetail(CustomerId) NOT NULL,
 ActualAmount DECIMAL(10,2) NOT NULL,
 CountryId INTEGER CONSTRAINT fk_CountryId_BillingDetail REFERENCES Country(CountryId) NOT NULL,
 PaymentType VARCHAR(20) NOT NULL, 
 ItemQuantity INTEGER NOT  NULL,
 ProductId INTEGER CONSTRAINT fk_ProductId_BillingDetail REFERENCES ProductInventory(ProductId) NOT NULL,
 PurchaseDate DATE NOT NULL,
 CONSTRAINT pk_BillIdBillItemId PRIMARY KEY(BillId,BillItemId)
);

-- COMMAND ----------

alter table BillingDetail add CONSTRAINT chk_ActualAmount_BillingDetail CHECK(ActualAmount>=0);
alter table BillingDetail add CONSTRAINT chk_PaymentType CHECK(PaymentType IN ('Cash on delivery','Net Banking','Credit/Debit  Card','Discount Coupon'));
alter table BillingDetail add CONSTRAINT chk_ItemQuantity CHECK(ItemQuantity>0);

-- COMMAND ----------

INSERT INTO BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(1,1,1,193000,1,'Net Banking',1,3, '2016-11-15');

-- COMMAND ----------

insert into BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(2,1,2,42199,2,'Net Banking',1,5,'2016-11-17');
INSERT INTO BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(3,1,3,60000,1,'Net Banking',1,4,'2016-11-21');
INSERT INTO BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(4,1,4,700,3,'Net Banking',1,8,'2016-11-19');
INSERT INTO BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(5,1,5,2500,2,'Net Banking',1,2,'2016-11-18');
INSERT INTO BillingDetail(BillId,BillItemId,CustomerId,ActualAmount,CountryId,PaymentType,ItemQuantity,ProductId,PurchaseDate) VALUES(6,1,7,200,4,'Net Banking',1,9,'2016-11-18');

-- COMMAND ----------

CREATE TABLE ShipDetail
(
 ShipId INTEGER CONSTRAINT pk_ShipId PRIMARY KEY,
 ShipAddress VARCHAR(500) NOT NULL,
 ShipZip VARCHAR(15) NOT NULL, 
 ShipCost DECIMAL(10,2)  NOT NULL ,
 SellerId INTEGER CONSTRAINT fk_SellerId_ShipDetail REFERENCES SellerDetail(SellerId) NOT NULL,
 CustomerId INTEGER CONSTRAINT fk_CustomerId_ShipDetail REFERENCES CustomerDetail(CustomerId) NOT NULL,
 ShipStatus VARCHAR(25) NOT NULL,
 ShipDate DATE NOT NULL,
 DeliveryDate DATE,
 CountryId INTEGER CONSTRAINT fk_CountryId_ShipDetail REFERENCES Country(CountryId) NOT NULL
);

-- COMMAND ----------

alter table ShipDetail add CONSTRAINT chk_ShipCost CHECK(ShipCost>=0);
alter table ShipDetail add CONSTRAINT chk_ShipStatus CHECK(ShipStatus IN ('Delivered','Consignee not available','Returned back to   seller','Dispatched','Yet to be dispatched'));
alter table ShipDetail add CONSTRAINT chk_ShipDateDeliveryDate CHECK(DeliveryDate>=ShipDate);

-- COMMAND ----------

--INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
--VALUES(1,'Mumbai, India','200002',200,1,1,'Delivered','2016-11-15','2016-11-18',1);
INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
VALUES(2,'Washington DC, USA','20009',5,2,2,'Dispatched','2016-11-18','2016-11-21',2);
INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
VALUES(3,'Delhi, India','100002',200,1,3,'Dispatched','2016-11-21','2016-11-23',1);
INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
VALUES(4,'Rio De Janeiro, Brazil','28640-000',50,3,4,'Delivered','2016-11-19','2016-11-22',3);
INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
VALUES(5,'Washington DC, USA','00501',5,2,5,'Delivered','2016-11-18','2016-11-21',2);
INSERT INTO ShipDetail(ShipId,ShipAddress,ShipZip,ShipCost,SellerId,CustomerId,ShipStatus,ShipDate,DeliveryDate,CountryId) 
VALUES(6,'London, England','WC2N',2,4,7,'Dispatched','2016-11-18','2016-11-24',4);


-- COMMAND ----------

select * from shipdetail

-- COMMAND ----------


