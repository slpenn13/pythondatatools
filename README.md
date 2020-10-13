# pythondatatools
Tools for extracting and managing data extraction. The library includes

## Version 0.3.1
* __BUG:__ post update of LIBOR load ted_spread and overnight rate switched
  - (__fixed__ -- Mon Oct 12 13:50 CDT 2020)
* __BUG:__ post update of LIBOR load loading all zeroed values (e.g. 8-31)
  - (__fixed__ Mon Oct 12 16:20 CDT 2020)
* __BUG:__ failed write to monthly economic for new data
  - (__fixed__ Mon Oct 12 20:48 CDT 2020)

## Version 0.1.1
* Added sql_alchemy interface (sql_alchemy_class.py)

## Bugs
* __BUG:__ crash of analysis LIBOR for mimssing items except ted spread

## Updates 0.4.0 (PROPOSED)
* Update method specific to monthly data

## The directory structure
* __bin__ : contains stand alone applications, e.g. fred_interface.py which can be run directly
    from command line
* __data__  : contains json files used to run stand alone applications from command line.
* __doc__ : additional dcoumentation beyond that in the code files
* __src__ : source diorectory containing classes and support routines
* __test__ : directory containing unit test framework

## Functionality
1. Mysql Interface
2. Slqachemy Inteface to sqlite (**under developement**, Wed Aug 12 10:17:17 CDT 2020)
2. API interface to St. Louis Federal Reserve data store (FRED)
  * Developed fred_interface python class. Now supports (1) Fred and (2) excel data loads. 
3. Intereface to ECB exchange rates store
4. HTML scraping tools
5. Wrapper class of MySQL interface:
  - interest_rates_interface
  - interest_rates_interface_extended
  - sql_class_base (SQL statement generator)
