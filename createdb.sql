create table movies(
  id number primary key,
  title varchar(255),
  imdbID varchar(255),
  spanishTitle varchar(255),
  imdbPictureURL varchar(255),
  year number,
  rtID varchar(255),
  rtAllCriticsRating number,
  rtAllCriticsNumReviews number,
  rtAllCriticsNumFresh number,
  rtAllCriticsNumRotten number,
  rtAllCriticsScore number,
  rtTopCriticsRating number,
  rtTopCriticsNumReviews number,
  rtTopCriticsNumFresh number,
  rtTopCriticsNumRotten number,
  rtTopCriticsScore number,
  rtAudienceRating number,
  rtAudienceNumRatings number,
  rtAudienceScore number,
  rtPictureURL varchar(255)
);

create table movie_genres(
movieID number, 
genre varchar(255)
);

create table movie_directors(
movieID number, 
directorID varchar(255),
directorName varchar(255)
);

create table movie_actors(
movieID number, 
actorID varchar(255),
actorName varchar(255),
ranking number
);


create table movie_locations(
  movieID number,
  location1 varchar(255),
  location2 varchar(255),
  location3 varchar(255),
  location4 varchar(255)
);

create table movie_tags(
  movieID number,
  tagID number,
  tagWeight number
);

create table movie_countries(
movieID number, 
country varchar(255)
);

create table tags(
id number, 
values varchar(255)
);

create index title_index on movies(title);
create index countries_index on movie_countries(country);
create index genre_index on movie_genres(genre);
create index locations_index on movie_locations(location1);

