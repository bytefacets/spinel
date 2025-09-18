# Example: Printer with Field Metadata
This example demonstrates the use of field metadata as it pertains to logging and printing
operators.

- The example starts by creating a table using an interface to describe the fields and their
   types. Note that there are annotations describing two numeric fields as having "Text"
   content.
- Next the table is connected to two different aggregations
- The three outputs (table and two aggregations) are each connected to a printer
- Example data is added to the table
- An example change is made to demonstrate the change behavior in the table and group by operators

### Example output

```text
SCH DataModel: 7 fields [0,DataCenter,Long][1,Amount,Double][2,Product,String][3,CountryCode,Short][4,Timestamp,Long][5,Date,Int][6,Key,Int]
SCH ByCountry: 3 fields [0,CountryCode,Short][1,Count,Int][2,TotalAmount,Double]
SCH ByDataCenter: 3 fields [0,DataCenter,Long][1,Count,Int][2,TotalAmount,Double]
ADD r____0: [DataCenter=useast1 ][Amount=7,093.3913][Product=product0][CountryCode=US][Timestamp=2025-09-17 22:40:44.268871][Date=2025-09-17][Key=0]
ADD r____1: [DataCenter=uswest2 ][Amount=7,006.1468][Product=product1][CountryCode=GB][Timestamp=2025-09-17 22:40:44.268925][Date=2025-09-17][Key=1]
ADD r____2: [DataCenter=euwest1 ][Amount=9,741.5756][Product=product2][CountryCode=DE][Timestamp=2025-09-17 22:40:44.268937][Date=2025-09-17][Key=2]
ADD r____3: [DataCenter=apne1   ][Amount=8,396.5220][Product=product3][CountryCode=FR][Timestamp=2025-09-17 22:40:44.268946][Date=2025-09-17][Key=3]
ADD r____4: [DataCenter=saeast1 ][Amount=9,061.8801][Product=product4][CountryCode=JP][Timestamp=2025-09-17 22:40:44.268954][Date=2025-09-17][Key=4]
ADD r____5: [DataCenter=useast1 ][Amount=9,597.4823][Product=product5][CountryCode=BR][Timestamp=2025-09-17 22:40:44.268961][Date=2025-09-17][Key=5]
ADD r____6: [DataCenter=uswest2 ][Amount=9,440.2152][Product=product0][CountryCode=US][Timestamp=2025-09-17 22:40:44.268973][Date=2025-09-17][Key=6]
ADD r____7: [DataCenter=euwest1 ][Amount=6,958.7198][Product=product1][CountryCode=GB][Timestamp=2025-09-17 22:40:44.268981][Date=2025-09-17][Key=7]
ADD r____8: [DataCenter=apne1   ][Amount=7,365.2047][Product=product2][CountryCode=DE][Timestamp=2025-09-17 22:40:44.268988][Date=2025-09-17][Key=8]
ADD r____9: [DataCenter=saeast1 ][Amount=8,977.9127][Product=product3][CountryCode=FR][Timestamp=2025-09-17 22:40:44.268994][Date=2025-09-17][Key=9]
ADD r___10: [DataCenter=useast1 ][Amount=5,969.2951][Product=product4][CountryCode=JP][Timestamp=2025-09-17 22:40:44.269001][Date=2025-09-17][Key=10]
ADD r___11: [DataCenter=uswest2 ][Amount=8,193.1843][Product=product5][CountryCode=BR][Timestamp=2025-09-17 22:40:44.269008][Date=2025-09-17][Key=11]
ADD r___12: [DataCenter=euwest1 ][Amount=5,999.1759][Product=product0][CountryCode=US][Timestamp=2025-09-17 22:40:44.269014][Date=2025-09-17][Key=12]
ADD r___13: [DataCenter=apne1   ][Amount=7,027.8640][Product=product1][CountryCode=GB][Timestamp=2025-09-17 22:40:44.269021][Date=2025-09-17][Key=13]
ADD r___14: [DataCenter=saeast1 ][Amount=8,266.6682][Product=product2][CountryCode=DE][Timestamp=2025-09-17 22:40:44.269027][Date=2025-09-17][Key=14]
ADD r___15: [DataCenter=useast1 ][Amount=6,431.7517][Product=product3][CountryCode=FR][Timestamp=2025-09-17 22:40:44.269034][Date=2025-09-17][Key=15]
ADD r___16: [DataCenter=uswest2 ][Amount=9,874.0770][Product=product4][CountryCode=JP][Timestamp=2025-09-17 22:40:44.269040][Date=2025-09-17][Key=16]
ADD r___17: [DataCenter=euwest1 ][Amount=8,310.8560][Product=product5][CountryCode=BR][Timestamp=2025-09-17 22:40:44.269055][Date=2025-09-17][Key=17]
ADD r___18: [DataCenter=apne1   ][Amount=5,314.8591][Product=product0][CountryCode=US][Timestamp=2025-09-17 22:40:44.269062][Date=2025-09-17][Key=18]
ADD r___19: [DataCenter=saeast1 ][Amount=8,014.3119][Product=product1][CountryCode=GB][Timestamp=2025-09-17 22:40:44.269079][Date=2025-09-17][Key=19]
ADD r___20: [DataCenter=useast1 ][Amount=6,055.1281][Product=product2][CountryCode=DE][Timestamp=2025-09-17 22:40:44.269087][Date=2025-09-17][Key=20]
ADD r____0: [CountryCode=US][Count=4][TotalAmount=27,848]
ADD r____1: [CountryCode=GB][Count=4][TotalAmount=29,007]
ADD r____2: [CountryCode=DE][Count=4][TotalAmount=31,429]
ADD r____3: [CountryCode=FR][Count=3][TotalAmount=23,806]
ADD r____4: [CountryCode=JP][Count=3][TotalAmount=24,905]
ADD r____5: [CountryCode=BR][Count=3][TotalAmount=26,102]
ADD r____0: [DataCenter=useast1 ][Count=5][TotalAmount=35,147]
ADD r____1: [DataCenter=uswest2 ][Count=4][TotalAmount=34,514]
ADD r____2: [DataCenter=euwest1 ][Count=4][TotalAmount=31,010]
ADD r____3: [DataCenter=apne1   ][Count=4][TotalAmount=28,104]
ADD r____4: [DataCenter=saeast1 ][Count=4][TotalAmount=34,321]
CHG r____5: [Amount=7,893.7767]
CHG r____5: [TotalAmount=24,398]
CHG r____0: [TotalAmount=33,443]
```