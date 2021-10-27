# How Long Does It Take Ordinary People To "Get Good" At Chess?

TL;DR: According to 5.5 years of data from 2.3 million players and 450 million games, most beginners will improve their rating by 100 lichess elo points in 3-6 months. Most "experienced" chess players in the 1400-1800 rating range will take 3-4 years to improve their rating by 100 lichess elo points. There's no strong evidence that playing more games makes you improve quicker.

## Table Of Contents

1. [Backstory](#backstory)
2. [ETL Process](#etl-process)
3. [Data Analysis](#data-analysis)
    - [How Long Does It Take To Improve At Chess?](#how-long-does-it-take-to-improve-at-chess)
    - [Does Playing More Games Make You A Better Chess Player?](#does-playing-more-games-make-you-a-better-chess-player)
    - [Misc. Questions Explored Out Of Curiosity](#misc-questions-explored-out-of-curiosity)
4. [How To Do What I Did](#how-to-do-what-i-did)


## Backstory

I've been a casual chess player for a few years now. Like most people who get into chess, one question that's been at the back of my mind is, like the title suggests, how long is it going to take me to actually get good at this game?

Fortunately for us, lichess.org, the largest open source online chess website, publishes all chess games played on their site freely available for the public to download (including elo rating data). This gives me just what I need to take a crack at this question.

But before we start getting into the data mining, we would first need to define what is "good at chess". Naturally, that's going to very from person to person. If your definition of "good" is never losing, that's never going to happen (unless you're picky about who you choose to play or you're a computer). If your definition of "good" is better than most everyday people you would find off the street, then you would probably get there after spending 30 minutes learning how the pieces move.

For what it's worth, the data says the 90th percentile rating is at 1927. On lichess.org/stat/rating/distribution/blitz, I believe the distribution is calculated from currently active players, raising the 90th percentile to 2075. For reference, GM Hikaru Nakamura (one of the world's top blitz players) was able to jump from total beginner (600 USCF ~1100 lichess) to the 90th percentile (1800 USCF, ~2000 lichess) in just 2 years (Jan 1995- Jan 1997). So best case scenario if you're a chess prodigy, you can get into the 90th percentile ballpark in 2 years of serious study.

Given the ambiguous nature of this question, I decided to focus on improvement rate rather than trying to answer something like how long it takes to go from total beginner to grandmaster.

There are a few reasons for this:

1. People who become grandmasters probably don't represent your typical everyday chess player. I'm more interested in how long it's going to take a "normal" person like me to improve.
2. While tournament games and rating histories of grandmasters (and other tournament players) are publically available, over the board chess is likely different than online chess, even if the time controls are the same. I'm more interested in getting better at online chess, which is where I usually play chess.
3. I think the rate of improvement is what people (myself included) are *really* interested in. I felt like the core of my question is actually "how long is it going to take until I see noticeable improvement in my chess skills?" It's also a helpful way to benchmark if your struggling at chess improvement is "normal" or if there's something wrong with your training plan.
4. It's unlikely I'm going to find many examples of people who went from beginner to grandmaster in the data. It takes at least 8-10 years to jump that gap in addition to extreme talent and near full-time study (back to point 1). Lichess data only goes back to 2013, 8 years of data as of this writing. Because it takes many years to develop strong players, there's a low chance such players have documented their entire progression on lichess. They would have had to start in the early days of lichess, back when it wasn't very popular. 
5. Even if I could get data spanning the many years required to build strong players, the amount of data would be *massive* even after stripping down the data to the bare minimum required, given the exponential growth of chess games being played on lichess. There are ways I could store and analyze the *massive* amount of data, but that's going to cost me an arm and a leg. Phrasing the question this way is going to allow me to work with the limit data I'm able to get my hands on.

So that brings me to the purpose of this project: to figure out the typical improvement rates for typical online chess players on lichess.org.

## ETL Process

Here's an outline of what I did to do this analysis:

1. Extract the data from database.lichess.org
2. Transform the data to the format I need it in
3. Load the processed data in a relational database
4. Analyze the data to answer my questions

Lichess has all their games available for download by month (e.g. January 2013). The problem is, the data in its raw format can't be queried easily, nor can it directly answer the question I'm interested in. 

Here's what the raw data looks like:

![Alt test](./screenshots/raw_data_sample.png?raw=true "Data Sample")

I believe it's an export from MongoDB, a NoSQL document database, which does not have a fixed schema. The problem with schemaless databases like MongoDB is they aren't designed for complex analytical queries (but great for application flexibility and scaling for big data). 

I decided to migrate the data into postgreSQL, a structured, relational database. That's going to allow me to run complex SQL queries to get the elo rating per player over time, which is really what I need to answer my question. It also gives me the flexibility to play around with the data for other questions that might pop up in the future.

To help manage the ETL process, I built a datapipeline using Airflow to schedule the downloading, processing, and deletion of each data file on lichess. The nice thing about Airflow is it comes with a web UI that helps you visually see where you're at in your data pipeline. It also provides logging and task status to help make troubleshooting/debugging a bit easier.

Here's a couple of screenshots of what the pipeline looks like:

![Alt test](./screenshots/airflow_dag.png?raw=true "Airflow DAG graph")
![Alt test](./screenshots/airflow_dag_closeup.png?raw=true "Airflow DAG graph closeup")

After a lot of troubleshooting and a few weeks of waiting for the data to download on my dinky 10 year old laptop, I got about 100 Gb of data loaded into postgres, spanning 5.5 years, 450 million games, and 2.3 million players.

Once that was done, I did the analysis with a bit of SQL and pandas then made the plots with seaborn/matplotlib.

## Data Analysis

### How Long Does It Take To Improve At Chess?

Now to the fun stuff.

What does the data say about chess improvement rate?

After extracting the data for elo per player over time (including games as white and black), filtering for one time control, calculating the monthly average, aligning everyone's starting dates, assigning the ratings into rating bins, and averaging the ratings by the rating bins, I get the plot below:

![Alt test](./analytics/plots/blitz_elo_over_time/blitz_elo_over_time_-1000_elo_cutoff.png?raw=true "blitz elo over time (all)")
 
I analyzed the data from the perspective of a player's monthly average which should be a better estimate of a player's playing strength than looking at the game-by-game elo fluctuation. I'm not particularly interested in cases of players who managed to jump 100 points in one afternoon blitz binge session. I believe those instances can be attributed to random chance rather than those players suddenly having a "eureka" moment that boosted their playing strength by 100 elo points overnight.

From the graph, it looks like improvement rate depends a lot on what your current elo is. As one might expect, lower elo ratings have the greatest opportunity to improve quickly, while higher elo ratings will take much longer to see improvement. Most players in the 800-1000 rating range (about 6% of players) will see their elo jump up 100 points in just a few months of activity. Most players in the 1600-2000 range (27% of players) will take 4 years or more to move up just 100 elo points. 

I'm not sure what the weird bump and dip is that happens around the 3 year mark. That may be an artifact of the data only containing 5.5 years of data, with datapoints heavily clustered around lower month counts (see player churn).

4 years just for 100 elo points? Seems a bit longer than I expected. But it is plausible.

There are players in the data with long histories of activity who have not improved their rating despite playing many games over the span of many years. See the player who's played the most games of all time on lichess:

![Alt test](./screenshots/german11_blitz.png?raw=true "german11 blitz stats")

But what if the mean ratings are being dragged down by the mass of "casual" players who aren't interested in improving and just play for fun? (Not that there's anything wrong with that). Is there a way to look at just the players who are serious about improving? I tried filtering the data to players who have gained at least 100 elo points since joining lichess:

![Alt test](./analytics/plots/blitz_elo_over_time/blitz_elo_over_time_100_elo_cutoff.png?raw=true)

There's a strange jump in rating in the first month for players in this category that is less prevalent in the entire dataset. It's possible this could be due to players starting out on lichess as underrated and quickly catching up in the first month. I'm going to ignore the first month when calculating the improvement rate.

From the chart, players in the 800-1000 range on average improve their elo by 100 points in just 1-2 months. Players in the 1600-2000 range improve their elo by 100 points in about 3-4 years, which isn't much different from the average of all players. One explanation for this is that most players at this rating range are already considered "serious" players and most of them "have what it takes" to improve at chess. Setting a cutoff in the data for 100 elo points of improvement is not as strong a filter for players in this range.

So it looks like for most people, improvement happens over long periods of time of consistent study/activity on the scale of months for beginners and years for experienced players.

But what about people who seem to have gone from beginner to ~2000 elo in just a couple of years? It's not possible to see what their progressions looked like from the plots above, yet these types of people *are* out there. I wanted to take a look at how many of these people actually exist. Are they really as rare as the above data seems to suggest?

Here's a heatmap showing the number of players who have raised their rating by X elo, divided up by their starting elo:

![Alt test](./analytics/plots/blitz_elo_over_time/heatmap_elo_gain_time.png?raw=true)

So it seems like there is a sizeable number of people who have made *significant* gains since they joined lichess as a beginner (~800-1200 elo). From the data for people in the 800-1200 starting range, there are about 120 people who have improved their elo by over 800 points since joining and about 1000 people who have improved their elo at least 500 points. However, it is important to note that the data does not filter out bots, cheaters, or smurfs and it's unclear what percentage such people contribute to these counts, but I suspect it's small.

Another piece of information that would be interesting to look at is how long did it take on average for these "outliers" to achieve such impressive gains?

Here's another heatmap showing the average time it took for people to achieve X elo gain, divided up by their starting elo:

![Alt test](./analytics/plots/blitz_elo_over_time/heatmap_elo_gain_count.png?raw=true)

The results were actually pretty surprising. It looks like these "outliers" seem to have made these gains in a little less than *2 years*! Amazingly, that's about the amount of time it took GM Hikaru Nakamura to bridge that gap when he was learning chess as a child. So it seems that there is hope for people looking to become strong players. With serious study and dedication, it looks like it's possible to make massive improvements in a reasonably short amount of time.

But one disclaimer I would like to add to that is to emphasize that these players are *outliers*. There are only a few thousand, *maybe* a few tens of thousands that have managed to accomplish this in comparison to the 820,000 player population included in the dataset. These people comprise just 1% of all players. While their results are *possible*, one could argue that these are not "ordinary" people.

The 0 elo gain column is also a bit deceptive, seemingly implying that more time = more elo gain given the low time values listed in that column. There's some *tiny* hint of truth to that, but I think this number is just being dragged down by the mass of newer players. If we remember some of the earlier analysis, most people who actually stay active for longer times do not actually improve to the extent potentially suggested by this heatmap.

### Does Playing More Games Make You A Better Chess Player?

But while I have the data here, let's take a moment to answer the question everyone's asking: **does playing more games make you a better chess player?**

Intuitively, the answer seems like it should be *yes*. Seems like experience should be a huge factor in someone's chess strength. It also seems like the go-to answer many of the top players recommend for improving at chess.

This is what the data says:

**Elo Rating vs. Number of Games Played**
![Alt test](./analytics/plots/elo_by_total_games_played_blitz.png?raw=true "elo vs number of games")
**Elo Rating vs. Number of Games Played Per Month**
![Alt test](./analytics/plots/elo_by_total_games_played_per_month_blitz.png?raw=true "elo vs number of games/month")
**Net Elo Gain vs. Number of Games Played**
![Alt test](./analytics/plots/elo_diff_by_total_games_played_blitz.png?raw=true "net elo gain vs number of games")
**Net Elo Gain Per Month vs. Number of Games Played Per Month**
![Alt test](./analytics/plots/elo_diff_per_month_by_total_games_per_month_blitz.png?raw=true "net elo gain/month vs number of games/month")

It seems like no matter how the data is sliced up, there does not seem to be a clear 1:1 linear correlation between improvement rate and number of games played. There's *maybe* a slight upward trend with elo gain vs games per month, but it seems to be a weak trend.

However, there does seem to be a sweet spot in the elo gain rate where a large portion of the players who have gained the most elo per month seem to cluster around 100-300 games per month, which comes out to a handful of games per day. That may be evidence that playing at least a few games here and there on a consistent basis will give the best chances at improving, but that could also be due to the fact that there are just more data points for players playing at that rate. 

In case you're curious about how people gained 100+ points per month, I checked the data and the majority of them are either cheaters, smurf accounts, or bots. For reference, the most improved player is (as of oct 2021) an 11 year old world chess champion from the Ukraine. His rate of improvement averaged out to 33 points per month over a period of 6 years.

Regardless, the data does not give any indications that bingeing chess games like it's a full time job is going to make you a stronger chess player any faster than playing a few games per day on your lunch breaks.

So what does make you a better chess player? I can only assume that it takes additional study beyond mindlessly playing chess games (like many other skills in life). Perhaps by practicing tactics, studying chess strategy, or studying your games. That seems to be what all the top players do. I would love to take a look at player's tactics data or studying habits in relation to their improvement rate, but that data is not readily available/easily obtainable, especially at the scale of this dataset. As a project manager would say, that's probably out of scope for this project.

### Misc. Questions Explored Out Of Curiosity

I had a lot of other ideas I wanted to explore while I had this data stored in my postgres database. That's the nice thing about relational databases. You have a lot of flexibility in how you can slice up and analyze the data.

These questions didn't fit with the narrative I kicked this project off with, so I'm just going to dump them here for you to browse through in case you're interested.

**How many games are played per time control?**

![Alt test](./analytics/plots/games_per_event.png?raw=true "games played per time control")

Seems like blitz is the most popular and almost no one plays correspondence. That surprised me a bit, I would have thought bullet would be the most popular just because more bullet games can be played than blitz games in a fixed amount of time.

**When do most people play chess?**

![Alt test](./analytics/plots/popular_play_times/games_by_time_of_day.png?raw=true)
![Alt test](./analytics/plots/popular_play_times/games_by_day.png?raw=true)
![Alt test](./analytics/plots/popular_play_times/games_by_time_and_day.png?raw=true)

Looks like 6pm UTC is when lichess players are the most active and 4am when they are least active. I guess most lichess players are from Europe because that lines up with when most people in European time zones would be getting off work and sleeping.

There's not much difference between the days of the week, but maybe Sunday has a slight edge over the other days.

**Who has played the most games in the dataset?**

The most active player in this dataset played 250,516 games (mostly bullet games), about 0.05% of all games in this database. There are only 10 players who have played over 100,000 games in this dataset (ordered from most to least):

1. german11
2. ribarisah
3. decidement
4. ASONINYA
5. bernes
6. Grga1
7. leko29
8. rezo2015
9. jiacomo
10. PILOTVE

**What are the highest/lowest ratings in the dataset?**

- Lowest: 630 by JAckermaan
- Highest (bot): 3233 by LeelaChessOfficial
- Highest (non-bot): 3196 by penguingim1 and Kuuhaku\_1
- Highest (non-bot blitz): 3031 by penguingim1, Ultimate\_SHSL\_Gamer, BehaardTheBonobo, galbijjim, Kuuhaku\_1, BeepBeepImAJeep, keithster

**How many games are played per elo band?** 

![Alt test](./analytics/plots/games_per_elo_bracket.png?raw=true)

**How many games does the median, average, maximum user play?** 

![Alt test](./analytics/plots/games_per_player.png?raw=true)

Here's the number of games players have played in this dataset:
- Median games played: 27
- Average games played: 482
- Maximum games played: 250,516

**Which elo bands analyze their games the most?**

![Alt test](./analytics/plots/pct_analyzed_per_elo_bracket.png?raw=true)

Most analyzed games are clustered at the top levels, probably by viewers or possibly by someone who decided to have a bunch of master games analyzed in bulk. 1800s seem to analyze the least. Perhaps at the higher levels, there is less need to rely on the computer and more reliance on player skill. Intuitively, that seems to make sense, at least from what I've seen strong chess players on youtube do. But then again, the difference is really small (only 1-2%) and probably is not significant.

**What is the typical change between players' starting and ending elo ratings?**

- **Most negative change (banned):** -1045.96. This account (laurent17320) was closed for violating terms of service (probably for intentionally losing games). They went from 1950 down to 895 elo.
- **Most negative change (not banned):** -1012.33. This account (Gyarados) went from 2145 elo down to 1133. I think this account used to be played by a strong player around the year 2016, then was handed off to a weaker player in 2017 who proceeded to drop the rating down to where it is now.
- **Median change:** 0. Most players don't play more than ~30 or so games before quitting lichess permanently, much less so staying active longer than 1 month. Hence why the most common rating change is 0.
- **Mean change:** 22. As expected, players should tend to get stronger over time, but the average is probably brought down by the majority of casual, non-serious chess players on lichess.
- **Most positive change:** 1404. This is held by the account PeshkaCh, as of this writing (Oct 2021) an 11 year old world chess champion from ukraine. Browsing some of the other top accounts, some of them are bots, many of them have been closed.
- **Highest positive change/month:** 897. I have explored accounts with the greatest elo change/month, however most of them are either bots with a very short activity timespan (ok\_zabu) or accounts that have been closed/banned probably for cheating (as expected). I don't think these accounts are particularly interesting. For reference, the account with the most positive change had an average increase of 33.45 elo/month.

![Alt test](./screenshots/peschkach_bio.png?raw=true)
![Alt test](./screenshots/peschkach.png?raw=true)

**How do most games finish?**

- Closed for fair play violation: 25,251 (0.01%)
- Game abandoned: 770,328 (0.17%)
- Lost due to time: 149,958,742 (33%)
- Finished normally: 62,791,173 (67%)

**How many games were played where black and/or white had a provisional rating (rating gain/loss of over 30 points)?**

26,778,555, about 6% of all games played

**Who played the most games for each time control?**

- german11: 211,781 bullet games
- jlomb: 58,966 blitz games
- tutubelezza: 22,674 rapid games  #note: Lichess categorized rapid as either blitz/classical initially, so data isn't entirely accurate here
- Snowden: 45,814 classical games
- lapos: 6,870 correspondence games

## How To Do What I Did

If you're interested in exploring some of this data yourself, I tried to make it easy to get everything setup on your machine. It will probably help if you're familiar with docker, python, SQL, and airflow.

First, you're going to need docker and docker compose installed on your computer. You can visit https://docs.docker.com/get-docker/ for instructions on how to do that. Once you have that installed, you can follow the steps below:

Clone this repository to your computer:

    git clone <link-from-this-repository> .

Give global write access to the src folder (for docker-compose)

    chmod +777 src

Run docker-compose up at the home directory of the repository where the docker-compose.yml file is. This will setup several docker containers for the airflow webserver, airflow worker, airflow scheduler, postgres databases (one for chess game data, one for airflow), redis database (for airflow), and a bash cli with python installed to run any scripts (mainly for plotting/running SQL queries)

    docker-compose up

Once the containers are up and running, you can get to the airflow webserver UI by going to localhost:8080 in your web browser. You can login with "username" and "password" (the default specified in the docker-compose.yml file).

You can click the switch to activate the DAG to start loading data into postgres. That's pretty much all you need to do to start loading data into your postgres database. You may want to modify the "airflow\_dag\_local.py" file if you want to download data from particular months. NOTE: There is a DAG that uses Kafka that I was playing with, but does not work without setting up kafka (complicated):

![Alt test](./screenshots/airflow_ui.png?raw=true)

You can use the cli container if you want to play with any of the python code (i.e. modify the code that transforms the data from lichess to postgres)

    docker ps
    docker container exec -it <python_cli_container_id> bash

Alternatively, you could just install the required python packages by installing from requirements.txt and run the scripts without docker:

    pip install -r requirements.txt

You can run SQL queries directly on the postgres database containing all the chess games you've downloaded (i.e. to peek at what the data looks like going into postgres):

    docker ps
    docker container exec -it <postgresql-chess_container_id> psql lichess_games username 

