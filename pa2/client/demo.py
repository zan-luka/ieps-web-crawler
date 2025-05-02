from flask import Flask, render_template_string, request
import requests
import json

app = Flask(__name__)

html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Vector Search Demo</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; line-height: 1.6; }
        .container { max-width: 800px; margin: 0 auto; }
        .search-form { margin-bottom: 20px; }
        input[type="text"] { width: 70%; padding: 8px; }
        select { padding: 8px; }
        button { padding: 8px 16px; background: #4285f4; color: white; border: none; }
        .result { margin-bottom: 15px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
        .score { font-weight: bold; color: #4285f4; }
        .highlight { background-color: #ffffcc; }
        .warning { background-color: #ffdddd; padding: 10px; border: 1px solid red; margin-bottom: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Vector Search Demo</h1>
        
        <div class="search-form">
            <form method="POST">
                <input type="text" name="query" placeholder="Enter your search query" value="{{ query }}" required>
                <select name="preset_query">
                    <option value="">-- Choose a predefined query --</option>
                    <option value="Google">Google</option>
                    <option value="starlink">starlink</option>
                    <option value="Apple">Apple</option>
                    <option value="Mozilla">Mozilla</option>
                    <option value="EU">EU</option>
                    <option value="asdfghjklqwerty">Unrelated nonsense</option>
                </select>
                <select name="metric">
                    <option value="cosine" {% if metric == 'cosine' %}selected{% endif %}>Cosine Similarity</option>
                    <option value="L1" {% if metric == 'L1' %}selected{% endif %}>L1 Distance</option>
                    <option value="inner" {% if metric == 'inner' %}selected{% endif %}>Inner Product</option>
                </select>
                <select name="model">
                    <option value="sloberta" {% if model == 'sloberta' %}selected{% endif %}>SloBERTa</option>
                    <option value="labse" {% if model == 'labse' %}selected{% endif %}>LaBSE</option>
                </select>
                <input type="number" name="limit" min="1" max="100" value="{{ limit or 5 }}" style="width: 80px;" title="Number of results">
                <button type="submit">Search</button>
            </form>
        </div>

        {% if low_confidence %}
            <div class="warning">
                <strong>Note:</strong> All returned results have low similarity. The query may be too vague or unrelated.
            </div>
        {% endif %}
        
        {% if results %}
            <h2>Search Results</h2>
            {% for result in results %}
                <div class="result">
                    <p class="score">{{ score_label }}: {{ result.score }}</p>
                    <p><strong>Segment Type:</strong> {{ result.segment_type }}</p>
                    <p><strong>Title:</strong> {{ result.title }}</p>
                    <p>{{ result.text | safe }}</p>
                </div>
            {% endfor %}
        {% elif error %}
            <div class="error">{{ error }}</div>
        {% endif %}
    </div>
</body>
</html>
"""

@app.route('/', methods=['GET', 'POST'])
def search():
    query = ""
    preset_query = ""
    metric = "cosine"
    model = "sloberta"
    limit = 5
    results = []
    error = None
    low_confidence = False
    score_label = "Similarity"

    if request.method == 'POST':
        query = request.form.get('query', '')
        preset_query = request.form.get('preset_query', '')
        metric = request.form.get('metric', 'cosine')
        model = request.form.get('model', 'sloberta')
        limit = int(request.form.get('limit', 5))

        if preset_query:
            query = preset_query

        if metric != "cosine":
            score_label = "Distance"

        if query:
            try:
                api_url = "http://localhost:5000/query"
                payload = {
                    "query": query,
                    "metric": metric,
                    "model": model,
                    "limit": limit
                }

                response = requests.post(api_url, json=payload)

                if response.status_code == 200:
                    data = response.json()
                    formatted_results = []
                    for item in data.get('results', []):
                        score_key = "similarity" if metric == "cosine" else "distance"
                        score = round(item.get(score_key, 0.0), 4)
                        formatted_results.append({
                            "text": item.get("segment", ""),
                            "score": score,
                            "title": item.get("title", ""),
                            "segment_type": item.get("segment_type", ""),
                            "page_id": item.get("page_id", "")
                        })

                    results = formatted_results
                    # Check if all scores are low (for cosine)
                    if metric == "cosine" and all(r["score"] < 0.3 for r in results):
                        low_confidence = True
                else:
                    error = f"API Error: {response.status_code} - {response.text}"

            except Exception as e:
                error = f"Error: {str(e)}"

    return render_template_string(
        html_template,
        query=query,
        preset_query=preset_query,
        metric=metric,
        model=model,
        limit=limit,
        results=results,
        error=error,
        score_label=score_label,
        low_confidence=low_confidence
    )

if __name__ == '__main__':
    app.run(debug=True, port=5001)
