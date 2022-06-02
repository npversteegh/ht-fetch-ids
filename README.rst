============
ht-fetch-ids
============

ht-fetch-ids is a set of Python command line tools for reconciling tabular files of bibliographic identifiers to HathiTrust volume IDs using the `HathiTrust Bibliographic API <https://www.hathitrust.org/bib_api>`_ intended to automate the creation of HathiTrust Research Center `Worksets <https://analytics.hathitrust.org/staticworksets>`_ based on Innovative Sierra catalog searches. It takes delimited multi-value tables of OCLC, LCCN, and ISBN/ISSN identifiers exported from Innovative Sierra using its `Create Lists <https://innovative.libguides.com/sierra/reports>`_ feature and prints the resulting matches to ``stdout`` in the CSV flavor of the user's choice.

Installation
============

To run ht-fetch-ids you'll need Python 3.9 or later available in a command line shell. ht-fetch-ids can be installed using pip directly from this GitHub page:

.. code-block::

   python -m pip install git+https://github.com/npversteegh/ht-fetch-ids@main

It's a good idea to install it in a Python virtual environment to prevent future conflicts. An example of doing this using Git Bash on Windows might look like:

.. code-block::

   mkdir ht-fetch-ids
   cd ht-fetch-ids
   python -m venv ht-fetch-ids
   source ht-fetch-ids/Scripts/activate
   python -m pip install git+https://github.com/npversteegh/ht-fetch-ids@main

use ``source ht-fetch-ids/bin/activate`` if you're on macOS or Linux and ``deactivate`` to deactivate the virtual environment.

Once ht-fetch-ids is installed you should have ``ht-fetch-ids``, ``print-col``, and ``extract-enumcrons`` ready in your shell whenever the virtual environment is active, which you can test by running help:

.. code-block::

   ht-fetch-ids --help

Usage
=====

First, you'll need a Sierra Create Lists export CSV of bibliographic identifiers to reconcile against HathiTrust::

  RECORD #(Bibliographic)	OCLC #	LC CARD #	ISBN/ISSN	TITLE	VOLUME
  "b10005651"	"637568"	"02017400"		"A complete history of Fairfield County, Ohio : 1795-1876 / by Hervey Scott"	
  "b10013313"	"637577"	"02012647"		"Pioneer period and pioneer people of Fairfield County, Ohio. By C. M. L. Wiseman ."	
  "b11082963"	"732890"	"13001175"		"A history of Cleveland, Ohio / by Samuel P. Orth ; With numerous chapters by special contributors"	"V.1";"V.1";"V.2";"V.2";"V.3";"V.3";"V. 1";"V. 2";"V. 3"

The ``ht-fetch-ids`` command will take this file as input and try to match it against the HathiTrust Bibliographic API using

#. The OCLC number
#. The LCCN
#. The first occurring ISBN or ISSN

in that order. By default it will take all volumes held by the library that has contributed the most volumes to HathiTrust using the most recent update to break ties. If the ``--vol-matcher`` option is specified ht-fetch-ids will try to match provided volume labels against HathiTrust volume labels (called enumcrons) using regular expressions and dead reckoning. It will extract series, part, volume, number, copy, and date spans from enumcron and volume strings then try to match them using the selected matcher. As of the time of last update three matchers have been drafted:

* ``exact`` which requires that the normalized volume labels match exactly (e.g. ``V1`` = ``Vol.1``, but ``V.1 NO.1`` != ``V.1``)
* ``1-span`` which requires that each volume match on the span with the highest percent coverage between the volume column labels and HathiTrust enumcrons
* ``2-span`` which requires that each volume match on the top two spans with the highest percent coverage between the volume column labels and HathiTrust enumcrons

``ht-fetch-ids`` has a ``--http-cache`` option to create a sqlite cache of HTTP requests that it can run against multiples times and a ``--delay`` option to reduce load on the HT API.

Example usage:

.. code-block::

   ht-fetch-ids --http-cache http-cache --vol-matcher 2-span sierra-export.txt > results.tsv

Once results have been obtained they can be dumped to a text file suitable for creating a HathiTrust Research Center Workset using ``print-col``

.. code-block::

   print-col --with-new-name "volume" results.tsv "htids" > workset.txt

To see what kind of coverage the regular expressions are getting from the enumcrons, you can ``print-col`` the enumcrons or volume labels from the results CSV to a text file and run ``extract-enumcrons`` on it:

.. code-block::

   print-col results.tsv enumcrons > enumcrons.txt
   extract-enumcrons enumcrons.txt > extracted-enumcrons.txt

or even simpler:

.. code-block::

   print-col results.tsv | extract-enumcrons > extracted-enumcrons.txt

which will show what spans are extracted from each enumcron or volume label::

  enumcron	seriesspan	volumespan	numberspan	partspan	datespan	copyspan	is_index	is_supplement	remainder	raw
  1901-1933 v.4 pt.1		(4, 4)		(1, 1)	(datetime.date(1901, 1, 1), datetime.date(1933, 12, 31))		False	False		1901-1933 v.4 pt.1
  Ser.2 v.17 (1891)	(2, 2)	(17, 17)			(datetime.date(1891, 1, 1), datetime.date(1891, 12, 31))		False	False		Ser.2 v.17 (1891)
  eastern division							False	False	eastern division	eastern division
