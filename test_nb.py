#%%
get_ipython().run_line_magic("load_ext", "sql")


#%%
get_ipython().run_line_magic("sql", "postgresql://student:student@127.0.0.1/sparkifydb")


#%%
get_ipython().run_line_magic("sql", "SELECT * FROM fact_songplay LIMIT 5;")


#%%
get_ipython().run_line_magic("sql", "SELECT * FROM dim_user LIMIT 5;")


#%%
get_ipython().run_line_magic("sql", "SELECT * FROM dim_song LIMIT 5;")


#%%
get_ipython().run_line_magic("sql", "SELECT * FROM dim_artist LIMIT 5;")


#%%
get_ipython().run_line_magic("sql", "SELECT * FROM dim_time LIMIT 5;")

#%% [markdown]
# ## REMEMBER: Restart this notebook to close connection to `sparkifydb`
# Each time you run the cells above, remember to restart this notebook to close the connection to your database. Otherwise, you won't be able to run your code in `create_tables.py`, `etl.py`, or `etl.ipynb` files since you can't make multiple connections to the same database (in this case, sparkifydb).

#%%

