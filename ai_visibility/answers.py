"""Answer Engine — restructures content for AI citation.

AI systems (ChatGPT, Perplexity, Google AI Overview) prefer:
  1. Direct answer in first 2-3 sentences
  2. Structured sections with clear headings
  3. Bullet points and lists
  4. FAQ format (question → answer)
  5. Factual, concise tone

This engine transforms SEO content into "answer-first" content
that AI systems can extract and cite.
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from ai_visibility.models import AnswerBlock
from models.business import BusinessContext

log = logging.getLogger(__name__)


ANSWER_PROMPT = """You are the AI Citation Agent.

Your goal: make content that AI systems (ChatGPT, Perplexity, Google AI) will cite as an answer.

Business: {business_name}
Service: {service}
City: {city}

Generate answer-optimized content for these questions:
{questions}

For EACH question, create:
1. direct_answer: 2-3 sentences that directly answer the question. Clear, factual, no fluff.
2. detailed_explanation: 150-200 words expanding on the answer with specifics.
3. bullet_points: 3-5 key takeaways as scannable bullets.

The content must:
- Lead with the answer (not background)
- Use simple, clear language
- Include specific numbers/facts when possible
- Sound authoritative but accessible
- Be structured for easy extraction by AI

Return ONLY JSON array:
[
  {{
    "question": "",
    "direct_answer": "",
    "detailed_explanation": "",
    "bullet_points": []
  }}
]"""


FAQ_GENERATION_PROMPT = """Generate the top 10 questions real people ask about this service.

Business: {business_name}
Service: {service}
City: {city}

Include:
- Cost questions ("How much does X cost in {city}?")
- Timing questions ("How long does X take?")
- Comparison questions ("X vs Y, which is better?")
- Emergency questions ("What to do when X happens?")
- Trust questions ("How to find a good X in {city}?")
- Process questions ("What happens during X?")

Return ONLY JSON array of strings (questions only):
["question 1", "question 2", ...]"""


class AnswerEngine:
    """Generates AI-optimized answer content."""

    def __init__(self):
        pass


    def _call(self, prompt: str, max_tokens: int = 4096) -> str:
        response = call_claude_raw(
            model=None,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return raw

    async def generate_faqs(self, business: BusinessContext) -> list[str]:
        """Generate the top questions people ask about this service."""
        prompt = FAQ_GENERATION_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
        )
        try:
            raw = self._call(prompt, max_tokens=1024)
            questions = json.loads(raw)
            log.info("answers.faqs  count=%d", len(questions))
            return questions
        except Exception as e:
            log.error("answers.faq_fail  err=%s", e)
            return []

    async def generate_answers(
        self,
        business: BusinessContext,
        questions: list[str] | None = None,
    ) -> list[AnswerBlock]:
        """Generate AI-optimized answers for a set of questions."""
        if not questions:
            questions = await self.generate_faqs(business)

        if not questions:
            return []

        question_block = "\n".join(f"- {q}" for q in questions[:10])
        prompt = ANSWER_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            questions=question_block,
        )

        try:
            raw = self._call(prompt)
            data = json.loads(raw)
            answers = [
                AnswerBlock(
                    question=item.get("question", ""),
                    direct_answer=item.get("direct_answer", ""),
                    detailed_explanation=item.get("detailed_explanation", ""),
                    bullet_points=item.get("bullet_points", []),
                    schema_type="FAQPage",
                )
                for item in data
            ]
            log.info("answers.generated  count=%d", len(answers))
            return answers
        except Exception as e:
            log.error("answers.generate_fail  err=%s", e)
            return []

    @staticmethod
    def answers_to_schema(answers: list[AnswerBlock]) -> dict:
        """Convert answers to FAQPage schema.org JSON-LD."""
        return {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": a.question,
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": a.direct_answer + " " + a.detailed_explanation,
                    },
                }
                for a in answers
            ],
        }

    @staticmethod
    def answers_to_html(answers: list[AnswerBlock]) -> str:
        """Render answers as HTML FAQ section."""
        lines = ['<section class="faq" itemscope itemtype="https://schema.org/FAQPage">']
        for a in answers:
            lines.append(f'  <div itemscope itemprop="mainEntity" itemtype="https://schema.org/Question">')
            lines.append(f'    <h3 itemprop="name">{a.question}</h3>')
            lines.append(f'    <div itemscope itemprop="acceptedAnswer" itemtype="https://schema.org/Answer">')
            lines.append(f'      <p itemprop="text">{a.direct_answer}</p>')
            lines.append(f'      <p>{a.detailed_explanation}</p>')
            if a.bullet_points:
                lines.append('      <ul>')
                for bp in a.bullet_points:
                    lines.append(f'        <li>{bp}</li>')
                lines.append('      </ul>')
            lines.append('    </div>')
            lines.append('  </div>')
        lines.append('</section>')
        return "\n".join(lines)
