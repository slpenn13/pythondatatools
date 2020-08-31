# pythondatatools
Tools for extracting and managing data extraction. The library includes

## Version 0.2.1

## Version 0.1.1
* Added sql_alchemy interface (sql_alchemy_class.py)

## Updates 0.3.0 (PROPOSED)
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
3. Intereface to ECB exchange rates store
4. HTML scraping tools
