# Lagos Real Estate Market Analysis

## Executive summary

This EDA looks at 652 unique Lagos real estate listings after removing 1 duplicated listing URL. The first thing that stands out is how skewed the price distribution is: the median listing is priced at about ₦115,020,632, but the upper tail stretches all the way to ₦15,000,000,000. That is why the log view matters so much in this project. It lets us see the middle of the market instead of letting a few luxury outliers dominate the whole story.

The second big takeaway is that location matters a lot. Lekki, Ikoyi, and Victoria Island carry most of the market activity, but the most expensive locations are not always the ones with the highest listing counts. That gap between volume and value is one of the most useful things this dataset reveals.

## 1. Starting with the basics

Before I trusted a single chart, I checked the structure of the data. The cleaned file is fairly neat: there are no missing values in the analysis frame, and the main categorical fields are populated consistently. That gave me enough confidence to move forward without over-explaining the cleaning stage.

| Metric | Value |
| --- | ---: |
| Raw rows | 653 |
| Unique listings used in EDA | 652 |
| Duplicate listing URLs | 1 |
| Missing values | 0 |
| Unique status values | 3 |
| Unique property kinds | 11 |
| Unique locations | 21 |
| Unique districts | 4 |

## 2. What the price distribution is really saying

Real estate pricing is almost never gentle, and Lagos is no exception. The raw price distribution is heavily right-skewed, which means a small number of very expensive listings stretch the scale and make everything else look compressed. When I switch to the log scale, the shape becomes much easier to read.

| Statistic | Value |
| --- | ---: |
| Minimum price | ₦150,000 |
| Q1 | ₦25,000,000 |
| Median | ₦115,020,632 |
| Q3 | ₦450,000,000 |
| Maximum | ₦15,000,000,000 |
| Luxury threshold (top 1%) | ₦4,647,000,000 |

The quartiles help tell the story in a cleaner way. Half of the listings sit below roughly ₦115,020,632, but the top quarter starts at ₦450,000,000. That is a big jump, and it tells me the market is not just expensive; it is segmented.

## 3. Where the market clusters

Once the price shape was clear, I wanted to know where the inventory actually lives. The district counts make the concentration obvious: Lekki leads with 340 listings, followed by Ikoyi and Victoria Island. Ajah is present too, but on a much smaller scale.

| District | Listings | Median price |
| --- | ---: | ---: |
| Lekki | 340 | ₦200,000,000 |
| Ikoyi | 168 | ₦50,000,000 |
| Victoria Island | 127 | ₦40,000,000 |
| Ajah | 17 | ₦300,000,000 |

At the location level, the picture gets even more nuanced. Lekki Phase 1 is the single biggest pocket of listings, but the priciest places are not always the busiest. Pinnock Beach Estate, Lekki County, Idado, Ikota, and Eko Atlantic all sit high on the pricing ladder even when they do not dominate the count chart.

## 4. What kind of stock dominates the market

The property mix is where the market starts feeling more concrete. Apartments make up the largest share of listings, which tells me this is a very urban, high-density market at the base level. But once I look at median price, the story changes. Maisonettes and detached duplexes move to the top, which is exactly what I would expect in a market where space, privacy, and status carry a premium.

| Property kind | Listings | Median price |
| --- | ---: | ---: |
| Apartment | 287 | ₦40,000,000 |
| Detached Duplex | 126 | ₦400,000,000 |
| Terrace | 80 | ₦180,000,000 |
| Semi Detached | 39 | ₦200,000,000 |
| Commercial | 36 | ₦25,000,000 |
| Mixed-Use Land | 32 | ₦4,250,000 |
| Maisonette | 23 | ₦450,000,000 |
| Penthouse | 17 | ₦97,200,000 |
| Residential Land | 6 | ₦137,500,000 |
| Joint Venture | 3 | ₦4,000,000 |
| Commercial Land | 3 | ₦1,100,000 |

The bedroom and bathroom signals move in the right direction, but they are not the whole story. A larger house is usually more expensive, yes, but location and property class matter just as much, and sometimes more.

## 5. Letting the data define the segments

Instead of inventing arbitrary price brackets, I split the market into quartiles. That produces a clean, interpretable segmentation: low, mid, high, and luxury. It is not a perfect social definition of affordability, but it is a practical way to see how the market stacks up.

| Segment | Listings | Median price | Avg. beds | Avg. baths |
| --- | ---: | ---: | ---: | ---: |
| Low | 185 | ₦13,000,000 | 1.90 | 1.90 |
| Mid | 141 | ₦43,400,000 | 3.35 | 3.35 |
| High | 175 | ₦280,000,000 | 3.31 | 3.31 |
| Luxury | 151 | ₦850,000,000 | 4.05 | 4.05 |

The luxury tail is small but important. Only 7 listings sit in the top 1%, and they cluster in premium locations like Ikoyi, Banana Island, Victoria Island, Ajah. Most of those listings are detached duplexes, mixed-use land, or other high-value formats that do not behave like the rest of the market.

## 6. A quick look at agents and concentration

This was not the main story, but it was worth checking. A handful of agents account for a large share of the listings, which suggests the market is quite concentrated behind the scenes. Jennifer, Peter, Esther, Ifunanya, and Adaeze are some of the most active names in the dataset.

That concentration is not automatically a bad thing, but it does mean the listing supply is not evenly distributed across the market. If you are analyzing inventory flow, it helps to remember that this is a broker-heavy dataset, not a perfectly random sample.

## 7. Correlations and caveats

The correlation matrix is useful, but only as a quick sanity check. Price moves positively with bedrooms and bathrooms, but the relationship is modest rather than dramatic. In this dataset, the strongest simple signal is still the mix of location and property class.

One small caveat is that bedrooms and bathrooms are almost perfectly aligned in many listings, so I would not over-interpret that pair as two independent signals. Real estate data often looks neat on paper while still being messy in the real world.

## 8. Final takeaway

If I had to explain the dataset in one sentence, I would say this: Lagos real estate in this sample is a concentrated, premium-heavy market with sharp location effects and a long luxury tail.

The important thing is not just that some listings are expensive. It is that the market is clearly stratified. A few districts carry most of the inventory, but a smaller set of locations carries the premium pricing. Apartments dominate the count, while detached duplexes and maisonettes dominate the money. That is the real structure hiding inside the dataset.

That is the story I would take away from the notebook: not just what is expensive, but how the market is organized, who dominates it, and where the real breaks in the pricing structure appear.

