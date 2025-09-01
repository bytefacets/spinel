# Example: GroupByExample
## Declaring an Aggregation

In this example
1. we take an orders table
2. add a projection to calculate the notional (i.e. the amount), then
3. aggregate by instrument, so that we have a result that's the instrument, 
count of orders, total quantity, and total amount. 
4. After the GroupBy, we calculate an average price over the group by dividing 
the total amount by the total quantity.

You'll see updates that modify the aggregation and projections, as well.

```text
+--------+     +------------------------+     +--------------+     +-------------+
| orders +-----+       projection       +-----+   group by   +-----+ projection  |
|        |     | (notional calculation) |     | (instrument) |     | (avg price) |
+--------+     +------------------------+     +--------------+     +-------------+
```
### Example output
```text
EnrichedOrderLog -- e0          SCH EnrichedOrder: 6 fields [0,Account,String][1,Price,Double][2,Qty,Int][3,InstrumentId,Int][4,OrderId,Int][5,Notional,Double]
  AggregationLog -- e0          SCH AvgPriceProjection: 6 fields [0,GroupId,Int][1,InstrumentId,Int][2,Count,Int][3,TotalQty,Int][4,TotalNotional,Double][5,AvgPrice,Double]
  AggregationLog -- e1          ADD r0     : [GroupId=0][InstrumentId=1][Count=1][TotalQty=500][TotalNotional=1295.0][AvgPrice=2.59]
EnrichedOrderLog -- e1          ADD r0     : [Account=Account1][Price=2.59][Qty=500][InstrumentId=1][OrderId=1][Notional=1295.0]
  AggregationLog -- e2          ADD r1     : [GroupId=1][InstrumentId=2][Count=1][TotalQty=600][TotalNotional=2268.0][AvgPrice=3.78]
EnrichedOrderLog -- e2          ADD r1     : [Account=Account2][Price=3.78][Qty=600][InstrumentId=2][OrderId=2][Notional=2268.0]
  AggregationLog -- e3          ADD r2     : [GroupId=2][InstrumentId=3][Count=1][TotalQty=600][TotalNotional=2640.0][AvgPrice=4.4]
EnrichedOrderLog -- e3          ADD r2     : [Account=Account0][Price=4.4][Qty=600][InstrumentId=3][OrderId=3][Notional=2640.0]
  AggregationLog -- e4          ADD r3     : [GroupId=3][InstrumentId=4][Count=1][TotalQty=500][TotalNotional=2830.0][AvgPrice=5.66]
EnrichedOrderLog -- e4          ADD r3     : [Account=Account1][Price=5.66][Qty=500][InstrumentId=4][OrderId=4][Notional=2830.0]
  AggregationLog -- e5          ADD r4     : [GroupId=4][InstrumentId=0][Count=1][TotalQty=300][TotalNotional=453.0][AvgPrice=1.51]
EnrichedOrderLog -- e5          ADD r4     : [Account=Account2][Price=1.51][Qty=300][InstrumentId=0][OrderId=5][Notional=453.0]
  AggregationLog -- e6          CHG r0     : [Count=2][TotalQty=900][TotalNotional=2227.0][AvgPrice=2.4744444444444444]
EnrichedOrderLog -- e6          ADD r5     : [Account=Account0][Price=2.33][Qty=400][InstrumentId=1][OrderId=6][Notional=932.0]
  AggregationLog -- e7          CHG r1     : [Count=2][TotalQty=700][TotalNotional=2645.0][AvgPrice=3.7785714285714285]
EnrichedOrderLog -- e7          ADD r6     : [Account=Account1][Price=3.77][Qty=100][InstrumentId=2][OrderId=7][Notional=377.0]
  AggregationLog -- e8          CHG r2     : [Count=2][TotalQty=1000][TotalNotional=4500.0][AvgPrice=4.5]
EnrichedOrderLog -- e8          ADD r7     : [Account=Account2][Price=4.65][Qty=400][InstrumentId=3][OrderId=8][Notional=1860.0000000000002]
  AggregationLog -- e9          CHG r0     : [TotalQty=700][TotalNotional=1709.0][AvgPrice=2.4414285714285713]
EnrichedOrderLog -- e9          CHG r0     : [Qty=300][Notional=777.0]
  AggregationLog -- e10         CHG r1     : [TotalQty=600][TotalNotional=2267.0][AvgPrice=3.7783333333333333]
EnrichedOrderLog -- e10         CHG r1     : [Qty=500][Notional=1890.0]
  AggregationLog -- e11         CHG r2     : [TotalQty=800][TotalNotional=3620.0][AvgPrice=4.525]
EnrichedOrderLog -- e11         CHG r2     : [Qty=400][Notional=1760.0000000000002]
  AggregationLog -- e12         CHG r3     : [TotalQty=300][TotalNotional=1698.0][AvgPrice=5.66]
EnrichedOrderLog -- e12         CHG r3     : [Qty=300][Notional=1698.0]
  AggregationLog -- e13         CHG r4     : [TotalQty=100][TotalNotional=151.0][AvgPrice=1.51]
EnrichedOrderLog -- e13         CHG r4     : [Qty=100][Notional=151.0]
```
Note that the AggregationLog prints first during the data changes because the order in which
the loggers are attached to the outputs: the aggregation is attached to the transformation
before the EnrichedOrderLog logger. The `e` parts of the log message indicate the "event count"
in the logger.

