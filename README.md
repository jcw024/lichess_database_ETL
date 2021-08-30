# Database Schema Design
A reference table is preferred over creating an ENUM type because it leaves the option open for us to expand/modify the values in our set in the future. That is not possible if we were to use ENUM and is generally recommended as best practice.

# Questions we want to answer
What is the average time it takes to reach each elo band? --> barchart (time vs. elo band)
What is the average rate of improvement at each elo band? --> barchart (rate of improvement vs. elo band)
What is the average time it takes to reach elo band X for the top 10% of fastest improving players (hereafter referred to as "good" players)? --> barchart (time vs. elo band)
do good players stay good or do they plateau? --> scatter (improvement rate vs. elo band)
What are the improvement rates by elo bands for the top 10% of players? --> barchart (improvement rate vs. elo band)
How many games do good/bad players play before reaching a certain elo? --> scatter chart (games played vs. elo)
What is the average time a good/bad player spends between games? --> barchart (improvement rate, avg time between games)
What time controls do good/bad players play at most often to improve? --> scatter plot (elo, duration, +color for time control)
Do good/bad players play specific time controls or focus on one? (time control vs. improvement rate)
Which elo bands analyze their games the most? --> barchart (% analyzed vs. elo bands)
Are good players more consistent than bad players? --> barchart comparison (games/month vs. improvement rate)
What does elo progression look like starting from 1000? --> scatter plot (elo, duration) transparent lines?
What percentage of players who have reached elo X reach elo Y? --> 3D surface (X, Y, %)
What percentage of players have been banned for cheating or other violation of TOS? --> pie chart
What is the median and average length of time players are active? --> dropoff chart (% active vs. duration)
