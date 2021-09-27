import pandas as pd
import seaborn as sns
import datetime as datetime
import numpy as np
import matplotlib.pyplot as plt
import re
import math
from pathlib import Path


def convert_day(x):
    if x == 0: x = "Sunday"
    elif x == 1: x = "Monday"
    elif x == 2: x = "Tuesday"
    elif x == 3: x ="Wednesday"
    elif x == 4: x = "Thursday"
    elif x == 5: x = "Friday"
    elif x == 6: x = "Saturday"
    return x

def group_date_times():
    """ad-hoc function to read the data and format it for plots
    that plot the number of games by time"""
    df = pd.read_csv("./query_out_storage/games_played_by_time_of_day.csv")
    #format hours and minutes into date_time
    df["time"] = df.apply(lambda x: datetime.time(int(x["hour"]), int(x["minute"])), axis=1)
    df["time_str"] = df["time"].apply(str)
    df["time_int"] = df.apply(lambda x: x["hour"]*60 + x["minute"], axis=1)

    #grouping into 48 "bins", one for each 30 minutes in a day
    df["time_group"] = pd.qcut(df["time_int"],48)
    df["day"] = df["day"].apply(convert_day)
    return df

def barplot_games_by_time_of_day(filename):
    df = group_date_times()
    df2 = df.groupby(by="time_group").sum()
    #ordering color palette by number_of_games
    pal = sns.color_palette("flare_r", df2.shape[0])
    rank = df2["number_of_games"].argsort().argsort()
    #creating barplot
    bar = sns.barplot(x=df2.index, y="number_of_games", data=df2, palette=np.array(pal)[rank])
    bar.set(xlabel='UTC Time', ylabel='Total Games Played (Millions)')
    bar.set_xticks(range(0,48,2))
    bar.set_xticklabels(range(0,24))
    bar.set_title("Total Number of Games Played by Time of Day")
    fig = bar.get_figure()
    fig.savefig(filename, dpi=300)

def barplot_games_by_day(filename):
    df = group_date_times()
    df2 = df.groupby(by="day").sum()
    df2 = df2.sort_values(by="number_of_games")
    bar = sns.barplot(x=df2.index, y="number_of_games", data=df2, palette="flare_r")
    bar.set(xlabel='Day of the Week', ylabel='Total Games Played (Millions)')
    bar.set_title("Total Number of Games Played by Day of the Week")
    fig = bar.get_figure()
    fig.savefig(filename, dpi=300)

def lineplot_games_by_time_and_day(filename):
    df = group_date_times()
    p = sns.relplot(data=df, x="time_int", y="number_of_games", hue="day", kind="line", linewidth=0.3)
    for ax in p.axes.flat:
        labels = ax.get_xticklabels()
        ax.set_xticks(ticks=range(0,24*60,60))
        ax.set_xticklabels(fontsize=8, labels=range(0,24))
        ax.set_title("Total Number of Games Played by Time and Day")
    p.set(xlabel='UTC Time', ylabel='Total Games Played')
    p.savefig(filename, dpi=300)

def pieplot_games_per_elo_band(filename):
    df = pd.read_csv("query_out_storage/total_games_per_elo_bracket.csv")
    df = df.sort_values(by="elo_bracket")
    #reordering > 600 and > 800 elo brackets
    df = pd.concat([df.iloc[[11],:], df.drop(11, axis=0)], axis=0)
    df = pd.concat([df.iloc[[11],:], df.drop(10, axis=0)], axis=0)
    #creating pie plot
    pct = 100*df.total_games/df.total_games.sum()
    labels = ["{0}: {1:1.2f} %".format(i,j) for i,j in zip(df.elo_bracket, pct)]
    pie, ax = plt.subplots(figsize=[10,6])
    patches, text = plt.pie(x=df.total_games, wedgeprops={'linewidth':1, 'linestyle':'-', 'edgecolor':'k'}, \
            pctdistance=1, startangle=90)
    ax.set_title("Total Number of Games Played per Elo Bracket")
    plt.legend(patches, labels, loc='center right', bbox_to_anchor=(1.4, .5),
           fontsize=8)
    plt.savefig(filename, dpi=300, bbox_inches='tight')

def pieplot_players_per_elo_band(filename):
    df = pd.read_csv("./query_out_storage/elo_diff_per_player.csv")
    df = df.groupby("band").count()["player"]
    df = df.reset_index()
    df = pd.concat([df.iloc[[7],:], df.drop(7, axis=0)], axis=0)
    pct = 100*df.player/df.player.sum()
    labels = ["{0}: {1:1.2f} %".format(i,j) for i,j in zip(df.band, pct)]
    pie, ax = plt.subplots(figsize=[10,6])
    patches, text = plt.pie(x=df.player, wedgeprops={'linewidth':1, 'linestyle':'-', 'edgecolor':'k'}, pctdistance=1, startangle=90)
    plt.legend(patches, labels, loc='center right', bbox_to_anchor=(1.4, .5),fontsize=8)
    ax.set_title("Total Number of Players per Elo Bracket")
    plt.savefig(filename, dpi=300, bbox_inches='tight')

def histogram_player_churn(filename):
    df = pd.read_csv("query_out_storage/total_games_per_player.csv")
    h, ax = plt.subplots(figsize=[10,6])
    n, bins, patches = plt.hist(df.total_games, bins=100, log=False, histtype='step')
    logbins = np.logspace(np.log10(bins[0]), np.log10(bins[-1]), len(bins))
    h, ax = plt.subplots(figsize=[10,6])
    plt.hist(df.total_games, bins=logbins, log=False, density=True, cumulative=1, histtype='step')
    plt.xscale('log')
    ax.set_title("Total Games Played per Player Cumulative Histogram")
    ax.set_xlabel("Number of Games Played")
    ax.set_ylabel("% of players who played x number of games or less")
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print("median games played per player: ", np.median(df.total_games))
    print("average games played per player: ", np.average(df.total_games))
    print("maximum games played by one player: ", max(df.total_games))

def barplot_pct_analyzed_per_elo_bracket(filename):
    df = pd.read_csv("query_out_storage/pct_analyzed_per_elo_bracket.csv")
    df["pct_analyzed"] = df["analyzed_games"]/df["total_games"]*100
    #reordering > 600 and > 800 elo brackets
    df = pd.concat([df.iloc[[11],:], df.drop(11, axis=0)], axis=0)
    df = pd.concat([df.iloc[[11],:], df.drop(10, axis=0)], axis=0)
    #ordering color palette by pct_analyzed
    pal = sns.color_palette("flare_r", df.shape[0])
    rank = df["pct_analyzed"].argsort().argsort()
    #creating barplot
    bar = sns.barplot(x="elo_bracket", y="pct_analyzed", data=df, palette=np.array(pal)[rank])
    bar.set(xlabel='elo_bracket', ylabel='pct_analyzed (%)')
    bar.set_title("Percentage of Games Analyzed by Elo Bracket")
    plt.setp(bar.xaxis.get_majorticklabels(), rotation=45, ha="right")
    fig = bar.get_figure()
    fig.savefig(filename, dpi=300, bbox_inches='tight')

def lineplot_elo_vs_days(filename):
    print("reading csv...")
    df = pd.read_csv("query_out_storage/total_blitz_games_per_player_over_time.csv")
    print("converting days since start...")
    df["days_since_start"] = df["days_since_start"].apply(lambda x: int(re.search(r'\d+', x).group(0)))
    df_starting_elo = df[df["days_since_start"] == 0]
    #assign rating bands to players
    for lower_elo_lim in range(800,2201,200):
        upper_elo_lim = lower_elo_lim + 199
        df_band = df_starting_elo[(df_starting_elo["min"] >= lower_elo_lim) & (df_starting_elo["min"] <= upper_elo_lim)]
        df.loc[df["player"].isin(df_band["player"]), "band"] = f"{lower_elo_lim} - {upper_elo_lim}"
    df = df.dropna()    #drop extreme elo values (600-800 and 2400+) due to low sample size
    print("plotting...")
    ax = sns.lineplot(x="days_since_start", y="min", hue="band", data=df)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0,box.width*.8, box.height])
    handles, labels = ax.get_legend_handles_labels()
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda x: int(x[0][0:4])))  #sort by elo number instead of str
    ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1,.5), title="starting elo")
    ax.set_xlabel("days since player's first stable rating")
    ax.set_ylabel('stable elo rating')
    ax.set_title('elo rating over time (95% confidence interval)')
    fig = ax.get_figure()
    print("saving figure...")
    fig.savefig(filename, dpi=300)

def lineplot_elo_vs_months(rating_diff_cutoff=-1000):
    filename = f"plots/blitz_elo_over_time/blitz_elo_over_time_{rating_diff_cutoff}_elo_cutoff.png"
    print("reading csv...")
    df = pd.read_csv("query_out_storage/total_blitz_games_per_player_over_time.csv")
    print("converting days since start...")
    df["days_since_start"] = df["days_since_start"].apply(lambda x: int(re.search(r'\d+', x).group(0)))
    df["months_since_start"] = df["days_since_start"].apply(lambda x: math.floor(x/30.5))
    df = df.groupby(["player","months_since_start"]).mean().reset_index()
    df_starting_elo = df[df["months_since_start"] == 0]
    #assign rating bands to players
    for lower_elo_lim in range(800,2201,200):
        upper_elo_lim = lower_elo_lim + 200
        df_band = df_starting_elo[(df_starting_elo["min"] >= lower_elo_lim) & (df_starting_elo["min"] < upper_elo_lim)]
        df.loc[df["player"].isin(df_band["player"]), "band"] = f"{lower_elo_lim} - {upper_elo_lim-1}"
    print("calculating starting and ending elo diff...")
    df = df.dropna()    #drop extreme elo values (600-800 and 2400+) due to low sample size
    #get diff between start and end ratings per player
    df_tmp = df.groupby(["player"]).min()
    df_start_elo = pd.merge(df[["player","min", "band","months_since_start"]], 
            df_tmp["months_since_start"], on=["player","months_since_start"])
    df_tmp = df.groupby(["player"]).max()
    df_end_elo = pd.merge(df[["player","min","months_since_start", "band"]], 
            df_tmp["months_since_start"], on=["player","months_since_start"])
    df_end_elo = df_end_elo.set_index("player")
    df_starting_elo = df_starting_elo.set_index("player")
    df_end_elo["diff"] = df_end_elo["min"] - df_starting_elo["min"]
    df_end_elo = df_end_elo.reset_index()
    #store statistics on diff data
    print("saving data in query_out_storage...")
    df_end_elo.to_csv("query_out_storage/elo_diff_per_player.csv")
    df_end_elo[["band","min","diff"]].groupby("band").median().to_csv("query_out_storage/elo_band_medians.csv")
    df_end_elo[["band","min","diff"]].groupby("band").mean().to_csv("query_out_storage/elo_band_means.csv")
    df_end_elo[["band","min","diff"]].groupby("band").min().to_csv("query_out_storage/elo_band_mins.csv")
    df_end_elo[["band","min","diff"]].groupby("band").max().to_csv("query_out_storage/elo_band_maxs.csv")
    df = df[df["player"].isin(df_end_elo["player"].loc[df_end_elo["diff"] > rating_diff_cutoff])]
    print(f"{df.shape[0]} rows remain after using a start-end elo diff cutoff of {rating_diff_cutoff}")
    n_players = df['player'].nunique() 
    print(f"{n_players} blitz players have gained {rating_diff_cutoff} rating points since their first stable rating")
    print("plotting...")
    ax = sns.lineplot(x="months_since_start", y="min", hue="band", data=df, alpha=0.6)
    plt.grid(b=True, axis='y', linestyle='--', color='black', alpha=0.3)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0,box.width*.9, box.height])
    ax.set_yticks(np.arange(1000,2600,200))
    handles, labels = ax.get_legend_handles_labels()
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda x: int(x[0][0:4])))  #sort by elo number instead of str
    ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1.05,.5), title="starting elo")
    ax.set_xlabel("months since player's first stable rating")
    ax.set_ylabel('stable elo rating')
    ax.set_title(f"elo vs. time: {n_players} users who gained > {rating_diff_cutoff} elo")
    fig = ax.get_figure()
    print(f"saving figure to {filename}...")
    fig.savefig(filename, dpi=300, bbox_inches='tight')
    return

def hexbin_elo_vs_games_played(filename, y="diff"):
    df_total_games = pd.read_csv("query_out_storage/total_games_per_player.csv")
    df_elo_diff = pd.read_csv("query_out_storage/elo_diff_per_player.csv")
    df_total_games = df_total_games.rename(columns={"white":"player"})
    df = df_total_games.merge(df_elo_diff, on="player")
    if y == "diff":
        g = sns.jointplot(data=df, x="total_games", y="diff", kind="hex", ylim=[-1000,1000], xscale='log', bins='log')
    else:
        g = sns.jointplot(data=df, x="total_games", y="min", kind="hex", xscale='log', bins='log')
    ax = g.ax_joint
    cbar = plt.colorbar(location='right')
    cbar.set_label('Number of players')
    ax.set_xlabel("Number of Games Played")
    if y == "diff":
        ax.set_ylabel('Net Elo Change')
    else:
        ax.set_ylabel('Elo Rating')
    fig = ax.get_figure()
    fig.savefig(filename, dpi=300, bbox_inches='tight')

if __name__ == "__main__":
    Path("./plots/popular_play_times").mkdir(parents=True, exist_ok=True)
    Path("./plots/blitz_elo_over_time").mkdir(parents=True, exist_ok=True)
    #barplot_games_by_time_of_day("./plots/popular_play_times/games_by_time_of_day.png")
    #lineplot_games_by_time_and_day("./plots/popular_play_times/games_by_time_and_day.png")
    #barplot_games_by_day("./plots/popular_play_times/games_by_day.png")
    #pieplot_games_per_elo_band("./plots/games_per_elo_bracket.png")
    #histogram_player_churn("./plots/games_per_player.png")
    #barplot_pct_analyzed_per_elo_bracket("./plots/pct_analyzed_per_elo_bracket.png")
    #lineplot_elo_vs_days("./plots/blitz_elo_over_time/blitz_elo_over_time.png")
    #lineplot_elo_vs_months()
    #lineplot_elo_vs_months(rating_diff_cutoff=100)
    #pieplot_players_per_elo_band("plots/players_per_elo_bracket.png")
    #hexbin_elo_vs_games_played("plots/elo_diff_by_total_games_played.png")
    hexbin_elo_vs_games_played("plots/elo_by_total_games_played.png", y="elo")
