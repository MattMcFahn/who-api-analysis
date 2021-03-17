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
#   - Setup data needed for app, and helper functions from database connection
##############################################################################
from dash_data_extraction import __get_years_tickvals, db_file, get_static_data_assets, __get_available_areas, __get_linegraph_data, __get_worldmap_data, __get_parameter_types_for_ind, __get_param_options


print('[DATA LOAD] Loading static assets...')
indicators, categories, areas = get_static_data_assets(db_file)

indicators_dict = [{'label': indicators.loc[i]['indicator_name'], 'value': indicators.loc[i]['indicator_code']} for i in indicators.index]
areas_dict = [{'label': areas.loc[i]['area_name'], 'value': areas.loc[i]['area_code']} for i in areas.index]
categories_dict = [{'label':categories.category_name[i], 'value':categories.category_name[i]} for i in categories.category_name.index]
print('[DATA LOAD] Complete <<< Launching app')

##############################################################################
#   - Setup data needed for app
##############################################################################

##############################################################################
#   - Main Dash app functions
##############################################################################
# Initalise Dash app  class
app = dash.Dash(__name__)
app.title = 'World Health Organisation: Dash data explorer'
def main():
    """Just testing"""
    # Set defaults to be loaded    
    app.layout = html.Div( # First \div element
    
    children=[
              html.Div(className='row',  # Define the row element. This'll have two cols
              children=[
                        html.Div(className='four columns div-user-controls',  # Define the left element
                                 children = [html.H6(children="Use dropdowns to select indicators and areas to display",
                                                     style={'marginBottom': 0, 'marginTop': 0}),
                                             html.Div([html.Label(['Choose category:'],style={'font-weight': 'bold', "text-align": "left"}),
                                                       dcc.Dropdown(id='category_dropdown',
                                                                    options=categories_dict,
                                                                    optionHeight=65,                    #height/space between dropdown options
                                                                    value="Mortality and Global Health Estimates",#dropdown value selected automatically when page loads
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
                                                                    optionHeight=65,                    #height/space between dropdown options
                                                                    value='WHOSIS_000014',              #dropdown value selected automatically when page loads
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
                                             # Optional 'parameter type' dropdown
                                             html.Div(id = 'parameter_type_dropdown_div',
                                                      children = [html.Label(['Choose parameter type:'],style={'font-weight': 'normal', "text-align": "left"}),
                                                       
                                                       dcc.Dropdown(id='parameter_type_dropdown',
                                                                    options=[{'label':0,'value':0}],
                                                                    optionHeight=65,                    #height/space between dropdown options
                                                                    value=None,                         #dropdown value selected automatically when page loads
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
                                                       ],className='DropDown',style={"display":"none"},
                                                      ),
                                             
                                             # Optional 'parameter' dropdown
                                             html.Div(id = 'parameter_dropdown_div',
                                                      children = [html.Label(['Choose parameter:'],style={'font-weight': 'normal', "text-align": "left"}),
                                                       
                                                       dcc.Dropdown(id='parameter_dropdown',
                                                                    options=[{'label':0,'value':0}],
                                                                    optionHeight=65,                    #height/space between dropdown options
                                                                    value=None,                         #dropdown value selected automatically when page loads
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
                                                       ],className='DropDown',style={"display":"none"}
                                                      ),
                                             
                                            ]
                                 ),
            
                        html.Div(className='eight columns div-for-charts bg-grey',  # Define the right element
                                 children = [
                                             html.Div(children=[html.H1(children="WHO Global Health Observatory: Dash Analytics", 
                                                                        style={'marginBottom': 25, 'marginTop': 65}),
                                                                
                                                                html.H2(children ="Explore GHO indicators across the world"),
                                                                dcc.Graph(id ='globe_graph',className = 'Graph', style={'width': '126vh', 
                                                                                                                        'height': '77.87vh'}),
                                                                
                                                                html.H2(children ="Single country explorer: Explore one country's performance over time"),
                                                                
                                                                html.Div([html.Label(['Choose area:'],style={'font-weight': 'bold', "text-align": "left"}),
                                                      
                                                                          dcc.Dropdown(id='area_dropdown',
                                                                                       options=areas_dict,
                                                                                       optionHeight=65,                    #height/space between dropdown options
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
                                                                dcc.Graph(id ='single_country_graph',className = 'Graph'),
                                                                ], style = {'backgroundColor':'rgb(50, 50, 50)'}
                                                      )
                                             ],
                                 )
                        ]
              )
              ]
    )
    
    return None

###############################################################################
# - Callbacks to update dropdowns
###############################################################################
### - Callback: Update area and parameter options, based on indicator
@app.callback(
    dash.dependencies.Output('area_dropdown', 'options'),
    dash.dependencies.Output('parameter_type_dropdown_div', 'style'),
    dash.dependencies.Output('parameter_dropdown_div', 'style'),
    dash.dependencies.Output('parameter_type_dropdown', 'options'),
    [dash.dependencies.Input('indicator_dropdown', 'value')])

def __restrict_areas_dropdown_add_param_dropdowns(ind_code):
    """Restrict to only areas with a value, and update parameter types. Needs to:
        > Update areas options
        > Update parameter_type dropdown visibility
        > Update parameter_type dropdown OPTIONS
        > Update parameter dropdown visibility
        """
    # Find present country codes, update areas_dict using this
    area_codes = __get_available_areas(ind_code)
    cut_areas = areas.loc[areas['area_code'].isin(area_codes)]
    cut_areas = cut_areas.sort_values(by = 'area_code').reset_index(drop = True)
    areas_dict = [{'label': cut_areas.loc[i]['area_name'], 'value': cut_areas.loc[i]['area_code']} for i in cut_areas.index]
    
    # Update visibility based on whether there are any parameter_type options
    param_types = __get_parameter_types_for_ind(ind_code)
    if param_types.empty:
        style = {"display":"none"}
        param_type_dict = [{'label':0,'value':0}]
    else:
        style = {"display":"block"}
        param_type_dict = [{'label':x,'value':x} for x in param_types['parameter_name'].values]
    
    return areas_dict, style, style, param_type_dict

### - Callback: Update indicator options, based on category
@app.callback(
    dash.dependencies.Output('indicator_dropdown', 'options'),
    [dash.dependencies.Input('category_dropdown', 'value')
     ])

def __restrict_indicator_dropdown(category_name):
    """Restrict to only indicators with a value"""
    cut_indicators = indicators.loc[indicators['category_name'] == category_name]
    
    indicators_dict = [{'label': cut_indicators.loc[i]['indicator_name'], 'value': cut_indicators.loc[i]['indicator_code']} for i in cut_indicators.index]
    return indicators_dict

### - Callback: Update parameter options based on parameter_type
@app.callback(
    dash.dependencies.Output('parameter_dropdown', 'options'),
    [dash.dependencies.Input('parameter_type_dropdown', 'value')
     ])

def __get_paramater_options(param_name):
    """Helper to get parameter options given the parameter name selected"""
    data = __get_param_options(param_name)
    
    params_dict = [{'label':x,'value':x} for x in data['parameter_value'].values]
    return params_dict

### - Callback: Update parameter value based on new indicator
@app.callback(
    dash.dependencies.Output('parameter_dropdown', 'value'),
    [dash.dependencies.Input('indicator_dropdown', 'value')
     ])

def __overwrite_param_value(ind_code):
    """Helper to get parameter options given the parameter name selected"""
    return None

###############################################################################
# - Callbacks to update dropdowns
###############################################################################


###############################################################################
# - Callbacks to update graphics
###############################################################################
### - Callback: Update line graph, based on inputs
@app.callback(
    dash.dependencies.Output(component_id = 'single_country_graph', component_property = 'figure'),
    [dash.dependencies.Input(component_id = 'area_dropdown', component_property = 'value'),
     dash.dependencies.Input(component_id = 'indicator_dropdown', component_property = 'value'),
     dash.dependencies.Input(component_id = 'parameter_dropdown', component_property = 'value'),
     ])

def __update_lineplot(area_code, ind_code, param_value):
    """Helper to render a lineplot for the area and indicator"""
    area_name = areas.loc[areas['area_code'] ==area_code].reset_index(drop=True).loc[0]['area_name']
    ind_name = indicators.loc[indicators['indicator_code'] ==ind_code].reset_index(drop=True).loc[0]['indicator_name']
    
    # Read the data
    data = __get_linegraph_data(area_code, ind_code, param_value)
    if data.empty:
        fig = go.Figure()
        fig.add_annotation(text = """No data for the selected area and indicator""")
        return fig
    
    if not param_value:
        title = f"""{area_name}: <br>{ind_name}"""
    else:
        title = f"""{area_name}: <br>{ind_name} - {param_value}"""
    
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
                          title = title,
                          xaxis = dict(tickmode = 'array',
                                       tickvals = __get_years_tickvals(years)),
                          plot_bgcolor='#ced4da',
                          paper_bgcolor='#ced4da'
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
                          title = title,
                          plot_bgcolor='#ced4da',
                          paper_bgcolor='#ced4da'
                          )
    return fig

### - Callback: Update world graph, based on inputs
@app.callback(
    dash.dependencies.Output(component_id = 'globe_graph', component_property = 'figure'),
     [dash.dependencies.Input(component_id = 'indicator_dropdown', component_property = 'value'),
      dash.dependencies.Input(component_id = 'parameter_dropdown', component_property = 'value'),
     ])

def __update_globe_graphic(ind_code, param_value):
    """Helper to render a world heat map based on the indicator selected"""
    indicator_name = indicators.loc[indicators['indicator_code'] == ind_code].reset_index(drop = True).loc[0].indicator_name
    
    data = __get_worldmap_data(ind_code, param_value)
    data.drop_duplicates(subset = ['area_code','year','numeric_value'], inplace = True)
    # Deal with no data cases. This does sometimes happen unfortunately (as data is available at a more granular level)
    if data.empty:
        fig = go.Figure(data = go.Choropleth(locations = areas['area_code']))
        fig.add_annotation(text = 'No data available at the top level. Select a lower granularity')
        return fig
    
    # Cut to just the latest year for all areas. Could pass this upstream into the SQL, but it's easier to handle the edge case here 
    if list(data.year.unique()) == [None]:
        max_year = 'Unknown'
    else:
        max_year = int(data['year'].max())
        max_yr_df = data.groupby('area_code')['year'].max().reset_index()
        data = max_yr_df.merge(data, on = ['area_code','year'], how = 'left', validate = 'one_to_one')
    
    if not param_value:
        title_text=f'{indicator_name}: <br>Data up to {max_year}'
    else:
        title_text=f'{indicator_name} - {param_value}: <br>Data up to {max_year}'
        
    text = 'Country: ' + data['area_name'] + '<br>Year: ' +\
            data['year'].astype(int).astype(str) +\
                '<br>Value: ' + data['numeric_value'].round(1).astype(str)
    
    fig = go.Figure(data=go.Choropleth(locations = data['area_code'],
                                       z = data['numeric_value'],
                                       text = text,
                                       colorscale = 'Blues',
                                       autocolorscale=False,
                                       reversescale=False,
                                       marker_line_color='darkgray',
                                       marker_line_width=0.5,
                                       colorbar_title = 'Value',
                                       hovertext = text,
                                       hoverinfo = 'text'
                                       )
                    )
    fig.update_layout(title_text=title_text,
                      geo=dict(showframe=False,
                               showcoastlines=False,
                               projection_type='equirectangular'
                               ),
                      plot_bgcolor='#ced4da',
                      paper_bgcolor='#ced4da'
                     )
    
    return fig
###############################################################################
# - Callbacks to update graphics
###############################################################################



# Startup app on running module
if __name__ == "__main__":
    main()
    app.run_server(debug=True)