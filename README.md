[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-lightgrey?logo=github)](https://github.com/sponsors/bytefacets)

# ByteFacets Spinel

Spinel is a framework for event-driven data processing especially for tabular data, either
embedded within a process or among multiple processes. Data is typically organized in rows 
and pushed through transformations that are set up by the author. Using various 
IPC and integration operators, data can be pushed to connected subscribers in different
processes, including other servers, client browsers, and desktop clients.

Example:

         +-----------+     +-----------+
         |   order   |     |  product  |
         |   table   |     |   table   |
         +---------+-+     +-+---------+
                   |         |                
               +---v---------v---+              
               |       join      |              
               | (on product_id) |              
               +--------+--------+              
               |                 |
        +------v------+   +------v------+              
        |    filter   |   |   group-by  |              
        |    (user)   |   |  (category) |              
        +-------------+   +-------------+           

Available Operators in the core library:
- KeyedTables (by various primitive types)
- Filter
- Join
- GroupBy
- Projection
  - field selection, aliasing, and reordering
  - calculated field
- Union
  - stacks tables into a single stream
- Prototype (establishes a schema until connected to an input)
- Conflation
- Logging

Memory efficiency is achieved using array-based storage, and a pointer-like system for
passing references to source data rows. Inputs are told of row change and field changes
