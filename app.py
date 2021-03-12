"""
 Main application file for a Dash dashboard using python
 
 -----------------------------------
 Created on Fri Mar 12 10:34:31 2021
 @author: matthew.mcfahn
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go

import sys
sys.path.append('/Users/matthew.mcfahn/Documents/GitHub/who-api-analysis')
sys.path.append('/Users/matthew.mcfahn/Documents/GitHub/who-api-analysis/99_Shared')


##############################################################################
#   - Setup data needed for app
##############################################################################
import sqlite_helpers
# TODO: Customise for a deployed and hosted environment
db_file = f'{sqlite_helpers.outdir}/who_data_model.sqlite3'
db_tables = sqlite_helpers.__get_table_schema(db_file)
dataframes = sqlite_helpers.__load_db_to_pandas(db_file, db_tables)
areas = dataframes['areas']
areas = areas.loc[areas['dimension'] == 'COUNTRY']
categories = dataframes['categories']

indicator_data = dataframes['indicator_data']
indicator_data = indicator_data.loc[indicator_data['numeric_value'].notna()]
indicator_data = indicator_data.loc[indicator_data['area_code'].isin(areas['area_code'].unique())]
present_indicators = list(indicator_data['indicator_code'].unique())
indicators = dataframes['indicator_info']
indicators = indicators.loc[indicators['indicator_code'].isin(present_indicators)]

indicators_dict = [{'label': indicators.loc[i]['indicator_name'], 'value': indicators.loc[i]['indicator_code']} for i in indicators.index]
areas_dict = [{'label': areas.loc[i]['area'], 'value': areas.loc[i]['area_code']} for i in areas.index]
categories_dict = [{'label':categories.loc[i]['category_name'], 'value':categories.loc[i]['category_id']} for i in categories.index]

##############################################################################
#   - Setup data needed for app
##############################################################################


##############################################################################
#   - Helper functions - Pull into a supporting module if needed
##############################################################################
def __get_years_tickvals(years):
    """
    Helper function to determine the year ticks for a graph based on how many 
    years are passed as input
    
    Parameters
    ----------
    years : list
        A list of the years for this dataset
    Returns
    -------
    year_ticks : list
        A list of places for tick marks on the years axis of the graph
    """
    min_year = int(min(years))
    max_year = int(max(years))
    delta = max_year - min_year
    if delta >= 80:
        stepsize = 10
    elif delta >= 40:
        stepsize = 5
    elif delta >= 16:
        stepsize = 2
    else:
        stepsize = 1
    year_ticks = list(range(min_year, max_year + stepsize, stepsize))
    return year_ticks

##############################################################################
#   - Helper functions - Pull into a supporting module if needed
##############################################################################


##############################################################################
#   - Main Dash app functions
##############################################################################
# Initalise Dash app  class
app = dash.Dash(__name__)
def main():
    """Just testing"""
    # Set defaults to be loaded    
    app.layout = html.Div( # First \div element
    
    children=[
              html.Div(className='row',  # Define the row element. This'll have two cols
              children=[
                        html.Div(className='four columns div-user-controls',  # Define the left element
                                 children = [html.P(children="Use dropdowns to select indicators and areas to display"),
                                             html.Div([html.Label(['Choose category:'],style={'font-weight': 'bold', "text-align": "left"}),
                                                       dcc.Dropdown(id='category_dropdown',
                                                                    options=categories_dict,
                                                                    optionHeight=35,                    #height/space between dropdown options
                                                                    value=25,                           #dropdown value selected automatically when page loads
                                                                    disabled=False,                     #disable dropdown value selection
                                                                    multi=False,                        #allow multiple dropdown values to be selected
                                                                    searchable=True,                    #allow user-searching of dropdown values
                                                                    search_value='',                    #remembers the value searched in dropdown
                                                                    placeholder='Please select...',     #gray, default text shown when no option is selected
                                                                    clearable=True,                     #allow user to removes the selected value
                                                                    style={'width':"100%"},             #use dictionary to define CSS styles of your dropdown
                                                                    ),                                  #'memory': browser tab is refreshed
                                                       
                                                            ],className='DropDown'),
                            
                                             html.Div([html.Label(['Choose indicator:'],style={'font-weight': 'normal', "text-align": "left"}),
                                                       
                                                       dcc.Dropdown(id='indicator_dropdown',
                                                                    options=indicators_dict,
                                                                    optionHeight=35,                    #height/space between dropdown options
                                                                    value='CM_03',                      #dropdown value selected automatically when page loads
                                                                    disabled=False,                     #disable dropdown value selection
                                                                    multi=False,                        #allow multiple dropdown values to be selected
                                                                    searchable=True,                    #allow user-searching of dropdown values
                                                                    search_value='',                    #remembers the value searched in dropdown
                                                                    placeholder='Please select...',     #gray, default text shown when no option is selected
                                                                    clearable=True,                     #allow user to removes the selected value
                                                                    style={'width':"100%"},             #use dictionary to define CSS styles of your dropdown
                                                                    # className='select_box',           #activate separate CSS document in assets folder
                                                                    # persistence=True,                 #remembers dropdown value. Used with persistence_type
                                                                    # persistence_type='memory'         #remembers dropdown value selected until...
                                                                    ),   
                                                       ],className='DropDown'),
                            
                                            html.Div([html.Label(['Choose area:'],style={'font-weight': 'normal', "text-align": "left"}),
                                                      
                                                      dcc.Dropdown(id='area_dropdown',
                                                                   options=areas_dict,
                                                                   optionHeight=35,                    #height/space between dropdown options
                                                                   value='GBR',                        #dropdown value selected automatically when page loads
                                                                   disabled=False,                     #disable dropdown value selection
                                                                   multi=False,                        #allow multiple dropdown values to be selected
                                                                   searchable=True,                    #allow user-searching of dropdown values
                                                                   search_value='',                    #remembers the value searched in dropdown
                                                                   placeholder='Please select...',     #gray, default text shown when no option is selected
                                                                   clearable=True,                     #allow user to removes the selected value
                                                                   style={'width':"100%"},              #use dictionary to define CSS styles of your dropdown
                                                                   ),                                  #'memory': browser tab is refreshed
                                                      ],className='DropDown'),
                                            ]
                                 ),
            
                        html.Div(className='eight columns div-for-charts bg-grey',  # Define the right element
                                 children = [
                                             html.Div(children=[html.H1(children="WHO Global Health Observatory: Dash Analytics"),
                                                                dcc.Graph(id ='single_country_graph',className = 'Graph'),
                                                                ]
                                                      )
                                             ],
                                 )
                        ]
              )
              ]
    )
    
    return None

### - Callback: Update area options, based on indicator
@app.callback(
    dash.dependencies.Output('area_dropdown', 'options'),
    [dash.dependencies.Input('indicator_dropdown', 'value')]
)
def __restrict_areas_dropdown(ind_code):
    # Find present country codes
    data = indicator_data.loc[indicator_data['indicator_code'] == ind_code]
    area_codes = list(data['area_code'].unique())
    
    cut_areas = areas.loc[areas['area_code'].isin(area_codes)]
    areas_dict = [{'label': cut_areas.loc[i]['area'], 'value': cut_areas.loc[i]['area_code']} for i in cut_areas.index]
    return areas_dict


### - Callback: Update indicator options, based on category (TEST: And area)
@app.callback(
    dash.dependencies.Output('indicator_dropdown', 'options'),
    [dash.dependencies.Input('category_dropdown', 'value'),
     dash.dependencies.Input('area_dropdown', 'value')
     ]
)
def __restrict_indicator_dropdown(category_id, area_code):
    
    cut_indicators = indicators.loc[indicators['category_id'] == category_id]
    data = indicator_data.loc[indicator_data['area_code'] == area_code]
    present_indicators = data['indicator_code'].unique()
    cut_indicators = cut_indicators.loc[cut_indicators['indicator_code'].isin(present_indicators)]
    
    indicators_dict = [{'label': cut_indicators.loc[i]['indicator_name'], 'value': cut_indicators.loc[i]['indicator_code']} for i in cut_indicators.index]
    return indicators_dict

### - Callback: Update graphic, based on inputs
@app.callback(
    dash.dependencies.Output(component_id = 'single_country_graph', component_property = 'figure'),
    [dash.dependencies.Input(component_id = 'area_dropdown', component_property = 'value'),
     dash.dependencies.Input(component_id = 'indicator_dropdown', component_property = 'value'),
     ])
def __update_plot(area_code, ind_code):
    """Helper to render a lineplot for the area and indicator"""
    area_name = areas.loc[areas['area_code'] ==area_code].reset_index(drop=True).loc[0]['area']
    ind_name = indicators.loc[indicators['indicator_code'] ==ind_code].reset_index(drop=True).loc[0]['indicator_name']
    
    data = indicator_data.loc[indicator_data['area_code'] == area_code]
    data = data.loc[data['indicator_code'] == ind_code]
    data.sort_values(by = 'year', inplace = True)
    
    years = data['year'].unique()
    if len(years) > 1:
        x = list(data["year"])
        y = list(data["numeric_value"])
        y_lower = list(data["low"])
        y_upper = list(data["high"])
        
        fig = go.Figure([
                go.Scatter(
                           x=x,
                           y=y,
                           line=dict(color='rgb(0,100,80)'),
                           mode='lines',
                           name='Value',
                           hovertemplate='Value: %{y:.1f}'+'<br>Year: %{x}')
                ,
                go.Scatter(
                           x=x+x[::-1], # x, then x reversed
                           y=y_upper+y_lower[::-1], # upper, then lower reversed
                           fill='toself',
                           fillcolor='rgba(0,100,80,0.2)',
                           line=dict(color='rgba(255,255,255,0)'),
                           showlegend=True,
                           name='Low & high bounds',
                           hoverinfo='skip')
                       ])
        fig.update_layout(xaxis_title='Year',
                          yaxis_title='Value',
                          title = f"""{area_name}: <br>{ind_name}""",
                          xaxis = dict(tickmode = 'array',
                                       tickvals = __get_years_tickvals(years))
                          )
    else:
        y = data[['low','numeric_value','high']].reset_index(drop = True)
        y = list(y.loc[0])
        groups = ['Low','Estimate','High']
        fig = go.Figure(data=[
                        go.Bar(name='SF Zoo', x=groups, y=y)
                        ])
        # Change the bar mode
        fig.update_layout(barmode='group',
                          xaxis_title='Area',
                          yaxis_title='Value',
                          title = f"""{area_name}: <br>{ind_name}"""
                          )
    return fig

# Startup app on running module
if __name__ == "__main__":
    main()
    app.run_server(debug=True)