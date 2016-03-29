#!/usr/bin/env python2.7

"""
Columbia W4111 Intro to databases
Example webserver

To run locally

    python server.py

Go to http://localhost:8111 in your browser


A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


#
DATABASEURI = "postgresql://ms5072:DYCHUK@w4111db.eastus.cloudapp.azure.com/ms5072"


#
# This line creates a database engine that knows how to connect to the URI above
#
engine = create_engine(DATABASEURI)


#
# eql_filter - a helper function which creates SQL equality filters if value is not empty
# keys - list of table keys
# values - list of filtered values
# pretext - what SQL pretext we use (by default AND, can be WHERE, ON, etc)
#
def eql_filter(keys,values,pretext=' AND '):
  filters = []
  for (k, v) in zip(keys,values):
    if v is not '':
      filters.append("{0}='{1}'".format(k,v))
  if filters:
    return pretext + ' AND '.join(filters)
  else:
    return ''

#
# Same as eql_filter, but now it does not add the value to the SQL string, 
# it adds it to another list that is also returned.
#
def eql_filter_safer(keys,values,pretext=' AND '):
  filters = []
  params = []
  for (k, v) in zip(keys,values):
    if v is not '':
      filters.append("{0}={1}".format(k,'%s'))
      params.append(v)
  if filters:
    return pretext + ' AND '.join(filters), params
  else:
    return '',None

#
# sql_builder - create sql queries from query token and additional information
# qtoken - The query token identifying the relevant SQL query
# info - List of additional information which can be used in the SQL query
#
def sql_builder(qtoken,info):
  if qtoken=='TZIPS':
    query = """
    SELECT 
      zip,
      sum(face_value) as total_debt,
      COUNT(DISTINCT p.acct_no) as no_properties,
      tax_year
    FROM Properties p, Tax_Certificates c 
    WHERE c.acct_no=p.acct_no
    {0}
    GROUP BY zip,tax_year
    ORDER BY 2 DESC
    """
  elif qtoken=='PROP':
    query = """
    SELECT p.acct_no ,p.zip ,p.address, COUNT(c.acct_no) as no_certs, SUM(c.face_value) as total_debt
    FROM Properties p, Tax_Certificates c
    WHERE c.acct_no=p.acct_no
    {0}
    GROUP BY p.acct_no ,p.zip ,p.address
    ORDER BY no_certs DESC
    """
  elif qtoken=='CERTS':
    query = """
    SELECT c.cert_id,c.acct_no,c.batch_id,c.auction_id,
          c.face_value,c.tax_year,c.cert_year,COUNT(b.bidder_id) as no_bids
    FROM Tax_Certificates c
    LEFT OUTER JOIN Bids b ON c.cert_id=b.cert_id
    {0}
    GROUP BY c.cert_id,c.acct_no,c.batch_id,c.auction_id,
          c.face_value,c.tax_year,c.cert_year
    ORDER BY cert_id
    """
  elif qtoken=='BIDS':
    query = """
    SELECT
      bidder_id,
      cert_id,
      acct_no,
      tax_year,
      bid_amount,
      winning
    FROM Bids
    {0}
    """
  elif qtoken=='BIDDER':
    query = """
    SELECT
      bidder_id,
      bidder_name,
      address,
      business_name,
      city,
      state,
      zip
    FROM Bidders
    {0}
    """
  elif qtoken=='AUCTIONS':
    query = """
    SELECT
      a.auction_id,
      a.issuer_url,
      a.start_date,
      a.end_date,
      a.min_deposit,
      a.bid_inc,
      a.min_bid,
      a.max_bid,
      i.county,
      (select count(*) from Batches where auction_id = a.auction_id) batch_count
    FROM Auctions a
    INNER JOIN Issuers i on i.issuer_url=a.issuer_url
    {0}
    """
  elif qtoken=='ISSUERS':
    query = """
    SELECT
      issuer_url,
      county,
      state
    FROM Issuers
    {0}
    """
  elif qtoken=='BATCHES':
    query = """
    SELECT
      b.batch_id,
      b.batch_time,
      b.auction_id,
      (select count(*) from Tax_Certificates c WHERE c.batch_id=b.batch_id AND c.auction_id=b.auction_id) as total_certificates
    FROM Batches b
    {0}
    """
  else:
    return false
  return query.format(*info)

def get_args(args,params):
  result=[]
  [result.append(args.get(p,'')) for p in params]
  return result

@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request 
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request

  The variable g is globally accessible
  """
  try:
    g.conn = engine.connect()
  except:
    print "uh oh, problem connecting to database"
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass

# Show static index page
@app.route('/')
def index():
  return render_template("index.html")

# Tax Certificates page
@app.route('/tax_certs/', methods=['POST', 'GET'])
def certs():
  params = ['acct_no','tax_year','batch_id','auction_id']
  sql_params=[]
  vals = get_args(request.args,params)
  params[0] = 'c.acct_no'
  info=[]
  sql_str_filter, _params = eql_filter_safer(params,vals, ' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(_params)
  
  query = sql_builder('CERTS',info)
  print "info =>",info
  print "built query =>", query

  cursor = g.conn.execute(query, tuple(sql_params))

  certificates = []
  for result in cursor:
    certificates.append(result)  # can also be accessed using result[0]
  cursor.close()

  context = dict(certs = certificates)

  return render_template("tax_certs.html", **context)

@app.route('/properties/', methods=['POST', 'GET'])
def properties():
  info=[]
  sql_params=[]
  params = ['acct_no','zip']
  vals = get_args(request.args,params)
  params[0] = 'c.acct_no'
  #Adds filter for zip
  sql_str_filter, _params = eql_filter_safer(params,vals)
  info.append(sql_str_filter)
  sql_params.append(_params)
  

  query = sql_builder('PROP',info)

  print "info =>",info
  print "built query =>", query

  cursor = g.conn.execute(query, tuple(sql_params))

  properties = []
  for result in cursor:
    properties.append(result)  # can also be accessed using result[0]
  cursor.close()

  context = dict(props = properties)

  return render_template("properties.html", **context)


@app.route('/topzips/', methods=['POST', 'GET'])
def zips():
  info=[]
  sql_params=[]
  selected_year = request.args.get('tax_year','')
  #Adds filter for tax year
  sql_str_filter, params = eql_filter_safer(['tax_year'],[selected_year])
  info.append(sql_str_filter)
  sql_params.append(params)

  query = sql_builder('TZIPS',info)

  print "info =>",info
  print "built query =>", query

  cursor = g.conn.execute(query, tuple(sql_params))

  zipcodes = []
  for result in cursor:
    zipcodes.append(result)  # can also be accessed using result[0]
  cursor.close()

  cursor = g.conn.execute("SELECT DISTINCT tax_year FROM Tax_Certificates ORDER BY tax_year DESC")  

  tax_years = []
  for result in cursor:
    tax_years.append(result)  # can also be accessed using result[0]
  cursor.close()
  if selected_year:
    selected_year = int(selected_year)
  else:
    selected_year = 0
  context = dict(zips = zipcodes, tyears = tax_years, selected_year=selected_year)


  return render_template("topzips.html", **context)

@app.route('/bids/', methods=['POST', 'GET'])
def bids():
  info=[]
  sql_params=[]
  
  sql_str_filter, params = eql_filter_safer(['cert_id'],[request.args.get('cert_id','')],' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(params)
  query = sql_builder('BIDS',info)

  print "info =>",info
  print "built query =>", query
  cursor = g.conn.execute(query, tuple(sql_params))

  allbids = []
  for result in cursor:
    allbids.append(result)  # can also be accessed using result[0]
  cursor.close()
  context = dict(bids = allbids)
  return render_template("bids.html", **context)
  
@app.route('/bidders/', methods=['POST', 'GET'])
def bidders():
  info=[]
  sql_params=[]
  
  sql_str_filter, params = eql_filter_safer(['bidder_id'],[request.args.get('bidder_id','')],' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(params)
  query = sql_builder('BIDDER',info)

  print "info =>",info
  print "built query =>", query
  cursor = g.conn.execute(query, tuple(sql_params))

  rows = []
  for result in cursor:
    rows.append(result)  # can also be accessed using result[0]
  cursor.close()
  context = dict(rows = rows)
  return render_template("bidders.html", **context)
  
@app.route('/auctions/', methods=['POST', 'GET'])
def auctions():
  info=[]
  sql_params=[]
  
  sql_str_filter, params = eql_filter_safer(['auction_id'],[request.args.get('auction_id','')],' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(params)
  query = sql_builder('AUCTIONS',info)

  print "info =>",info
  print "built query =>", query
  cursor = g.conn.execute(query, tuple(sql_params))

  rows = []
  for result in cursor:
    rows.append(result)  # can also be accessed using result[0]
  cursor.close()
  context = dict(rows = rows)
  return render_template("auctions.html", **context)

@app.route('/issuers/', methods=['POST', 'GET'])
def issuers():
  info=[]
  sql_params=[]
  
  sql_str_filter, params = eql_filter_safer(['issuer_url'],[request.args.get('issuer_url','')],' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(params)
  query = sql_builder('ISSUERS',info)

  print "info =>",info
  print "built query =>", query
  cursor = g.conn.execute(query, tuple(sql_params))

  rows = []
  for result in cursor:
    rows.append(result)  # can also be accessed using result[0]
  cursor.close()
  context = dict(rows = rows)
  return render_template("issuers.html", **context)


@app.route('/batches/', methods=['POST', 'GET'])
def batches():
  info=[]
  sql_params=[]
  
  sql_str_filter, params = eql_filter_safer(['batch_id'],[request.args.get('batch_id','')],' WHERE ')
  info.append(sql_str_filter)
  sql_params.append(params)
  query = sql_builder('BATCHES',info)

  print "info =>",info
  print "built query =>", query
  cursor = g.conn.execute(query, tuple(sql_params))

  rows = []
  for result in cursor:
    rows.append(result)  # can also be accessed using result[0]
  cursor.close()
  context = dict(rows = rows)
  return render_template("batches.html", **context)


@app.route('/login')
def login():
    abort(401)
    this_is_never_executed()


if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using

        python server.py

    Show the help text using

        python server.py --help

    """

    HOST, PORT = host, port
    print "running on %s:%d" % (HOST, PORT)
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
