Schema versioning is the practice of tracking and controlling changes to a
database/data structure over time. Here, it means evolving the message models in
messages.py, which are the contract between the exchange and each client, 
without forcing the exchange and its clients to upgrade at the same moment. This
is necessary because the exchange and clients may get updated at different
times. The codebase does this by only ever adding fields with defaults. To add a
field without breaking older clients, the field must have a default, ideally
None, so that messages without it still validate and "not sent" can't be
confused with a real value. A new client can then still validate messages from
an older exchange that never sends it, and older clients receiving messages 
from a new exchange are unaffected, because pydantic ignores undeclared
fields by default. It is also important not to change an existing field's name,
type, or meaning, or tighten a constraint the sender might violate. Isinstance
checks on dicts only check that the frame is a dict, not which message it is
or whether it is well formed. The discriminated union reads the type literal and
chooses the model to validate it against, then validates every field's presence,
type, and constraints using a single TypeAdapter.validate_json call. This means
that malformed or unknown messages fail there, specifying the error with a
ValidationError, rather than surfacing later as a KeyError or a bug in the 
trading logic. Additionally, what comes out is a typed object rather than a
dict that every consumer must check by hand.



### Sample Output from ws_client ###

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.08 | 25.29     7       
       3     25.01 | 25.39     20      
      20     24.98 | 25.49     30      
      10     24.94 | 

ARENA10  mid 297.16  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.16  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.17  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.17  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.18  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    5       
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.07  spread 0.32
 bid qty       bid | ask       ask qty 
      10    302.91 | 303.23    5       
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.18  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.02  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.44 | 144.60    10      
      20    142.86 | 145.18    20      
      30    142.29 | 145.75    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.03  spread 0.31
 bid qty       bid | ask       ask qty 
      10    302.88 | 303.19    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.19  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.10  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.52 | 144.68    10      
      20    142.94 | 145.26    20      
      30    142.37 | 145.83    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.03  spread 0.31
 bid qty       bid | ask       ask qty 
      10    302.88 | 303.19    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.81  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.15  spread 0.06
 bid qty       bid | ask       ask qty 
       2     25.12 | 25.18     1       
      10     25.05 | 25.25     10      
       3     25.01 | 25.35     20      
      20     24.95 | 25.45     30      
      10     24.88 | 

ARENA10  mid 297.19  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.10  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.52 | 144.68    10      
      20    142.94 | 145.26    20      
      30    142.37 | 145.83    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.03  spread 0.31
 bid qty       bid | ask       ask qty 
      10    302.88 | 303.19    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.83  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.09  spread 0.07
 bid qty       bid | ask       ask qty 
      10     25.05 | 25.12     3       
       3     25.01 | 25.18     1       
      20     24.95 | 25.25     10      
      10     24.88 | 25.35     20      
      30     24.85 | 25.45     30      

ARENA10  mid 297.21  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

NVDA  mid 144.10  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.52 | 144.68    8       
      20    142.94 | 145.26    20      
      30    142.37 | 145.83    30      
       1    140.20 | 
       2    139.25 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.10  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.52 | 144.68    8       
      20    142.94 | 145.26    20      
      30    142.37 | 145.83    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.03  spread 0.31
 bid qty       bid | ask       ask qty 
      10    302.88 | 303.19    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 698.50  spread 1.04
 bid qty       bid | ask       ask qty 
       2    697.98 | 699.02    10      
      10    697.01 | 700.02    20      
       1    696.81 | 700.48    10      
      20    696.01 | 701.02    30      
       1    695.79 | 702.48    3       

AMD  mid 172.83  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.07  spread 0.11
 bid qty       bid | ask       ask qty 
       3     25.01 | 25.12     3       
      10     25.00 | 25.18     1       
      20     24.90 | 25.20     10      
      10     24.85 | 25.30     20      
      30     24.80 | 25.40     30      

ARENA10  mid 297.21  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

MSFT  mid 431.67  spread 1.10
 bid qty       bid | ask       ask qty 
       1    431.12 | 432.22    9       
      10    430.21 | 433.22    20      
      20    429.21 | 434.22    30      
      30    428.21 | 
       1    428.19 | 

NVDA  mid 144.10  spread 1.16
 bid qty       bid | ask       ask qty 
      10    143.52 | 144.68    8       
      20    142.94 | 145.26    20      
      30    142.37 | 145.83    30      
       1    140.20 | 
       2    139.25 | 

TSLA  mid 303.03  spread 0.31
 bid qty       bid | ask       ask qty 
      10    302.88 | 303.19    10      
      10    302.22 | 304.23    10      
      20    301.22 | 305.23    20      
      30    300.22 | 306.23    30      
       1    297.99 | 307.39    3       

AMZN  mid 214.45  spread 0.23
 bid qty       bid | ask       ask qty 
      10    214.34 | 214.57    10      
      10    213.63 | 215.09    3       
       1    212.94 | 215.36    10      
       1    212.79 | 216.22    20      
      20    212.77 | 217.08    30      

GOOGL  mid 180.64  spread 0.09
 bid qty       bid | ask       ask qty 
      10    180.60 | 180.69    3       
      10    180.50 | 180.92    1       
      20    179.87 | 181.01    2       
       3    179.36 | 181.22    5       
      30    179.15 | 181.40    1       

META  mid 568.54  spread 1.09
 bid qty       bid | ask       ask qty 
      10    567.99 | 569.08    3       
      10    567.75 | 569.50    10      
      20    566.75 | 569.63    3       
      30    565.75 | 569.76    10      
       3    563.61 | 570.76    20      

NFLX  mid 697.50  spread 0.97
 bid qty       bid | ask       ask qty 
      10    697.01 | 697.98    3       
       1    696.81 | 699.02    10      
      20    696.01 | 700.02    20      
       1    695.79 | 700.48    10      
      30    695.01 | 701.02    30      

AMD  mid 172.82  spread 0.00
 bid qty       bid | ask       ask qty 
                   | 176.16    10      
                   | 176.86    20      
                   | 177.55    30      
                   | 180.31    5       
                   | 180.44    5       

INTC  mid 25.07  spread 0.11
 bid qty       bid | ask       ask qty 
       3     25.01 | 25.12     3       
      10     25.00 | 25.18     1       
      20     24.90 | 25.20     10      
      10     24.85 | 25.30     20      
      30     24.80 | 25.40     30      

ARENA10  mid 297.22  spread 0.00
 bid qty       bid | ask       ask qty 

ORCA  mid 29.80  spread 0.15
 bid qty       bid | ask       ask qty 
       3     29.72 | 29.87     10      
      10     29.62 | 29.91     5       
       3     29.52 | 29.99     20      
      20     29.50 | 30.11     30      
      30     29.38 | 

AAPL  mid 224.92  spread 0.10
 bid qty       bid | ask       ask qty 
      10    224.87 | 224.97    1       
      10    224.81 | 225.05    10      
       1    224.17 | 225.11    3       
       3    224.13 | 225.54    1       
      20    223.97 | 226.27    2       

Done after 30s (578 frames outside the union skipped)
(.venv) (base) brettsuazo@Bretts-MacBook-Air homework3 % 