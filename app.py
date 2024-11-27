from flask import Flask, render_template, request, redirect, url_for
from faker import Faker
from steam_reviews.reviews import SteamReviews

app = Flask(__name__)

steam_reviews = SteamReviews()
faker = Faker()

DISTINCT = steam_reviews.get_distinct()
DISTINCT_SORTED = steam_reviews.get_distinct_sorted(asc=True)

REVIEWS_PER_PAGE = 10  # Number of reviews to display per page


# Function to get game details and reviews
# Function to get game details and reviews
def get_game_details(game_name):
    # If game_name is a tuple, get the first element (the name)
    if isinstance(game_name, tuple):
        game_name = game_name[0]

    # Retrieve real reviews using `get_reviews()`
    real_reviews = steam_reviews.get_reviews(game_name)

    # Debug: Print the first few reviews to check the structure
    print("DEBUG: Real Reviews Structure:", real_reviews[:5])

    # Generate reviews with random names using `Faker`
    reviews_with_names = []
    for review in real_reviews:
        # Since reviews are Row objects, use dot notation to access fields
        review_data = {
            "user": faker.name(),
            "author_steamid": review["author_steamid"],
            "review": getattr(review, "review", "No review available"),
            "time_created": getattr(review, "timestamp_created", "Unknown"),
            "votes_up": getattr(review, "votes_up", "No"),
            "voted_up": getattr(review, "voted_up", "No"),
        }
        reviews_with_names.append(review_data)

    # Calculate positive review percentage
    total_reviews = len(real_reviews)
    positive_reviews = sum(1 for review in real_reviews if getattr(review, "voted_up", 0) == 1)
    positive_percentage = (positive_reviews / total_reviews * 100) if total_reviews > 0 else 0

    # Format positive percentage to two decimal places
    positive_percentage = round(positive_percentage, 2)

    # Get recommended games based on the current game
    recommended_games = steam_reviews.get_game_recommendations(game_name)

    # Return reviews, positive percentage, and recommended games
    return reviews_with_names, positive_percentage, recommended_games


@app.route("/", methods=["GET", "POST"])
def home():
    sort_by = request.args.get("sort_by", "ascending")  # Get sorting preference from query params, default is ascending

    if sort_by == "ascending":
        filtered_games = steam_reviews.get_distinct_sorted(asc=True)[:10]
    elif sort_by == "descending":
        filtered_games = steam_reviews.get_distinct_sorted(asc=False)[:10]
    elif sort_by == "top_rated_ascending":
        filtered_games = steam_reviews.get_top_overall_rated_games(asc=True)[:10]
    elif sort_by == "top_rated_descending":
        filtered_games = steam_reviews.get_top_overall_rated_games(asc=False)[:10]
    else:
        filtered_games = steam_reviews.get_distinct_sorted(asc=True)[:10]  # Default sorting

    if request.method == "POST":
        search_query = request.form.get("game_name")
        if search_query:
            # Filter games based on the search query (case-insensitive search)
            filtered_games = [
                game
                for game in DISTINCT_SORTED
                if search_query.lower() in game[0].lower()
            ][:10]

    # Ensure filtered_games contains only game names, not tuples
    filtered_game_names = [game[0] if isinstance(game, tuple) else game for game in filtered_games]

    return render_template("home.html", games=filtered_game_names, sort_by=sort_by)


@app.route("/game/<game_name>")
def game_details(game_name):
    page = request.args.get("page", 1, type=int)

    # Get reviews and positive percentage from `get_game_details()`
    reviews, positive_percentage, recommended_games = get_game_details(game_name)

    # Pagination logic
    start = (page - 1) * REVIEWS_PER_PAGE
    end = start + REVIEWS_PER_PAGE

    paginated_reviews = reviews[start:end]
    total_pages = (len(reviews) + REVIEWS_PER_PAGE - 1) // REVIEWS_PER_PAGE  # Total number of pages

    return render_template(
        "game_details.html",
        reviews=paginated_reviews,
        game_name=game_name,
        page=page,
        total_pages=total_pages,
        positive_percentage=positive_percentage,
        recommended_games=recommended_games,
    )


@app.route("/user/<author_steamid>")
def user_details(author_steamid):
    # Get recommended games for the user using ALS model
    user_recommendations = steam_reviews.get_user_recommendations(author_steamid)

    return render_template("user_details.html", user_recommendations=user_recommendations, author_steamid=author_steamid)


if __name__ == "__main__":
    app.run(debug=True)
