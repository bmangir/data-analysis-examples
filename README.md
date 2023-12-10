# DATA ANALYSIS EXAMPLES
## DESCRIPTION
Analyse the input data and find the requirements.
## LIBRARIES
>[Flask](https://pypi.org/project/Flask/), [request](https://pypi.org/project/requests/), json
### USAGES
#### Flask Usage 
>app = Flask(__name__)<br>
@app.route("/data/", methods=["GET"])<br>
#### request Usage
>exp = request.args.get("exp", type=int, default=1)
#### json Usage
> json.dumps(exp_data, indent=4)
## FILES
`data`
files include input data as json format.<br>
`sql`
files include sql queries which are creating table of input data and insert the data.<br>
`example-data-result`
contains example result of queries that are expected.<br>
## AUTHORS
[Berkant Mangır](https://www.linkedin.com/in/berkant-mang%C4%B1r-47b939221/)