"""Claude prompts for content generation — used by execution handlers."""

GBP_POST_PROMPT = """Generate a Google Business Profile post.

Business: {business_name}
Service: {service}
City: {city}

Requirements:
- 120-150 words
- Include service keyword + city naturally
- Include a clear CTA (call, book, visit)
- Sound human, not robotic
- Engaging opening line

Return ONLY JSON:
{{
  "text": "",
  "cta": "",
  "image_prompt": ""
}}"""


REVIEW_RESPONSE_PROMPT = """Write a review response for this business.

Business: {business_name}
Service: {service}
City: {city}
Review rating: {rating}/5
Review text: {review_text}
Reviewer name: {reviewer_name}

Requirements:
- Mention the service naturally
- Mention the city once
- Personalize to the reviewer's comments
- Express genuine gratitude
- Under 80 words
- Professional but warm tone

Return ONLY the response text, no JSON."""


SERVICE_PAGE_PROMPT = """Create a local service page.

Business: {business_name}
Service: {service}
City: {city}
Target keyword: {keyword}

Include:
- Title tag (under 60 chars, SEO optimized)
- Meta description (under 160 chars)
- H1 heading
- 100 word intro paragraph
- 200 word service description section
- FAQ section (3 questions + answers)
- CTA paragraph

Return ONLY JSON:
{{
  "title": "",
  "meta_description": "",
  "h1": "",
  "intro": "",
  "service_section": "",
  "faqs": [{{"question": "", "answer": ""}}],
  "cta": ""
}}"""


ARTICLE_PROMPT = """Write an SEO article.

Business: {business_name}
Target keyword: {keyword}
City: {city}
Topic: {topic}

Requirements:
- 800-1200 words
- Problem explanation → solution breakdown → service CTA
- Include keyword naturally (3-5 times)
- Local signals (city mention 2-3 times)
- Trustworthy local business tone
- H2 subheadings every 200 words

Return ONLY JSON:
{{
  "title": "",
  "meta_description": "",
  "content_html": "",
  "word_count": 0
}}"""


META_UPDATE_PROMPT = """Optimize the title tag and meta description for this page.

Current title: {current_title}
Current meta: {current_meta}
Page URL: {page_url}
Target keyword: {keyword}
Business: {business_name}
City: {city}

Requirements:
- Title under 60 chars, keyword near front
- Meta under 160 chars, compelling + keyword
- Include city
- Drive click-through

Return ONLY JSON:
{{
  "title": "",
  "meta_description": ""
}}"""


OUTREACH_EMAIL_PROMPT = """Write a backlink outreach email.

Our business: {business_name} ({website})
Target site: {target_site}
Target contact: {contact_name}
Our content/resource: {resource}

Requirements:
- Personalized to the target site
- Clear value exchange (what they get)
- Direct ask for link/mention
- Under 120 words
- Professional but friendly tone

Return ONLY JSON:
{{
  "subject": "",
  "body": ""
}}"""


INTERNAL_LINK_PROMPT = """Suggest internal links to add to this page.

Page URL: {page_url}
Page content summary: {content_summary}
Available pages to link to:
{available_pages}

Requirements:
- Find natural anchor text opportunities
- Only suggest contextually relevant links
- Max 3-5 links per page
- Don't force links where they don't fit

Return ONLY JSON array:
[
  {{
    "anchor_text": "",
    "link_to": "",
    "context": "sentence where link should be placed"
  }}
]"""
