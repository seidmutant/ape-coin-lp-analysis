# Ape Coin: Liquidity Pool Analysis

Medium post: TO DO

## Overview

If imitation is a form of flattery; than here it is in its highest form. In the pages below, I'm going to replicate an analysis by @0xfbifemboy where he deep dives into ApeCoin liquidity pools. Read his original report first, and then join me to explore the queries answering one of the most tangled questions of all: wen to stake?

@0xfbifemboy's report "A Study on Apes: Profits vs. Impermanent Losses":
Medium: https://crocswap.medium.com/a-study-on-apes-profits-vs-impermanent-losses-56667e4029e6
Github: https://github.com/CrocSwap/blog-posts/blob/main/drafts/apecoin.md

What I love about this analysis is that it gives real data to a dilemma that is rare, (mostly) profitable but is also very murky on what the best play is. The dilemma is - you've been airdropped a new coin, what do you do? Sell, hold, or contribute to a liquidity pool? Or another situation that is more common but doesn't have the same "free money" guarantee - there is a hyped airdrop or coin launch, do you buy in?

So why replicate this paper? Two reasons: the first is a selfish one - I'm doing this to get my head around exactly what @0xfbifemboy did. The second one is to write and publish the queries in case anyone wants to do their own analysis using them. The original report is a masterclass in high quality analysis; hopefully we can all learn from a closer read.

On the second point - Uniswap data on Dune is a labyrinth of information. To put it lightly: value locations and required manipulations are not necessary intuitive, and there are multiple table locations, etc. to pull from. I want to call out that I'm thankful for all the Dune wizards that have made their queries public. I'll make sure to shout out dashboards along the way.

Finally, @0xfbifemboy's post is a mammoth of a report so I'm splitting it into two separate articles. This first article (this one) will focus on price and liquidity positions, the second will continue with fees and impermanent loss.

Ok here we go, let's learn from some giga-brains…
