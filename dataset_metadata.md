# Yelp Dataset JSON Schema Documentation

[cite_start]This document outlines the exact structure and data types for the six JSON files provided in the Yelp Open Dataset [cite: 100-103]. The agent should use these exact field names for all database schema design and querying tasks.

## 1. business.json
[cite_start]Contains business data including location data, attributes, and categories [cite: 107-108].
* [cite_start]`business_id`: string, 22 character unique string business id [cite: 110-111]
* [cite_start]`name`: string, the business's name [cite: 112-113]
* [cite_start]`address`: string, the full address of the business [cite: 114-115]
* [cite_start]`city`: string, the city [cite: 116-117]
* [cite_start]`state`: string, 2 character state code [cite: 118]
* [cite_start]`postal code`: string, the postal code [cite: 119-120]
* [cite_start]`latitude`: float, latitude [cite: 121-122]
* [cite_start]`longitude`: float, longitude [cite: 123-124]
* [cite_start]`stars`: float, star rating, rounded to half-stars [cite: 125]
* [cite_start]`review_count`: integer, number of reviews [cite: 126-127]
* [cite_start]`is_open`: integer, 0 or 1 for closed or open [cite: 128-129]
* [cite_start]`attributes`: object, business attributes to values (can be nested objects) [cite: 130-141]
* [cite_start]`categories`: array of strings of business categories [cite: 143-147]
* [cite_start]`hours`: object, key day to value hours (24hr clock) [cite: 148-156]

## 2. review.json
[cite_start]Contains full review text data including the user_id and business_id [cite: 158-159].
* [cite_start]`review_id`: string, 22 character unique review id [cite: 161-162]
* [cite_start]`user_id`: string, 22 character unique user id [cite: 163]
* [cite_start]`business_id`: string, 22 character business id [cite: 164]
* [cite_start]`stars`: integer, star rating [cite: 165-166]
* [cite_start]`date`: string, formatted YYYY-MM-DD [cite: 167-168]
* [cite_start]`text`: string, the review itself [cite: 169-172]
* [cite_start]`useful`: integer, number of useful votes received [cite: 174]
* [cite_start]`funny`: integer, number of funny votes received [cite: 175]
* [cite_start]`cool`: integer, number of cool votes received [cite: 176]

## 3. user.json
[cite_start]User data including the user's friend mapping and metadata [cite: 177-178].
* [cite_start]`user_id`: string, 22 character unique user id [cite: 181-182]
* [cite_start]`name`: string, the user's first name [cite: 183-184]
* [cite_start]`review_count`: integer, number of reviews they've written [cite: 185-186]
* [cite_start]`yelping_since`: string, formatted YYYY-MM-DD [cite: 187]
* [cite_start]`friends`: array of strings, user_ids of friends [cite: 188-192]
* [cite_start]`useful` / `funny` / `cool`: integers, votes sent by user [cite: 193-197]
* [cite_start]`fans`: integer, number of fans [cite: 198-199]
* [cite_start]`elite`: array of integers, years the user was elite [cite: 200-203]
* [cite_start]`average_stars`: float, average rating of all reviews [cite: 204-205]
* [cite_start]`compliment_*`: integers, various compliment metrics (hot, more, profile, cute, list, note, plain, cool, funny, writer, photos) [cite: 206-217]

## 4. checkin.json
Checkins on a business [cite: 218-219].
* [cite_start]`business_id`: string, 22 character business id [cite: 221]
* [cite_start]`date`: string, comma-separated list of timestamps (YYYY-MM-DD HH:MM:SS) [cite: 222-223]

## 5. tip.json
[cite_start]Tips written by a user on a business (shorter than reviews) [cite: 225-226].
* [cite_start]`text`: string, text of the tip [cite: 229-230]
* [cite_start]`date`: string, formatted YYYY-MM-DD [cite: 231]
* [cite_start]`compliment_count`: integer [cite: 232-233]
* [cite_start]`business_id`: string, 22 character business id [cite: 234]
* [cite_start]`user_id`: string, 22 character unique user id [cite: 235]

## 6. photo.json
[cite_start]Photo data and classifications [cite: 236-237].
* [cite_start]`photo_id`: string, 22 character unique photo id [cite: 239-240]
* [cite_start]`business_id`: string, 22 character business id [cite: 241-242]
* [cite_start]`caption`: string, photo caption [cite: 243-244]
* [cite_start]`label`: string, category (e.g., food, drink, menu, inside, outside) [cite: 237, 245-246]