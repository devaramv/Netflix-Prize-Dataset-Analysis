#### Problem Statement

Find the *M* movies which have been rated the highest across all users (of movies which have been rated by at least *R*users). (If there's a tie for the *Mth* spot, prefer most recent publication then alphabetical order of title.) These are the "top movies."

Of users who have rated all top *M* movies, find the *U* users which have given the lowest average rating of the *M*movies. (If there's a tie for the *Uth* spot, prefer users with the lower ID.) These are the "contrarian users."

For the *U* contrarian users, find each user's highest ranked movie. (If there's a tie for each user's top spot, prefer most recent publication then alphabetical order of title.)

Prepare a CSV report with the following columns:

- User ID of contrarian user
- Title of highest rated movie by contrarian user
- Year of release of that movie
- Date of rating of that movie

Note: You will be graded on:

- Correctness
- Completeness of solution
- Unit tests
- Documentation
- Clarity of code

Note: *M*, *U*, and *R* should be configurable. The recommended default values for the parameters are *M* = 5, *U* = 25, and *R* = 50.