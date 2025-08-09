import json
from openai import OpenAI

# Load your OpenAI API key from JSON file
with open("/Users/kevinnovanta/backend_for_ai_agency/Creds/gpt_key.json") as f:
    openai_key = json.load(f)["api_key"]

client = OpenAI(api_key=openai_key)

def build_prompt(lead):
    company_name = lead.get("Company Name", "Unknown Company")
    industry_info = lead.get("Custom 2", "Unknown Industry")
    offer_summary = lead.get("Overview", "provide services")

    prompt = f"""
### Company Info:
- Name: {company_name}
- Industry: {industry_info}
- Offer Summary: {offer_summary}

### Your Task:
Write a short, warm cold outreach email (under 110 words) inviting the company to a quick discovery call (free workflow audit) where we identify bottlenecks and propose a tailored automation plan.

### Script Rules:
1. Do NOT re-explain what they do; show you understand their offer by referencing it naturally.
2. Mention our company — Outbound Accelerator — as specialists in advanced AI workflows and ops automation.
3. Use this context line once: "We help other {industry_info.lower()} businesses streamline messy processes, cut manual work, and increase booked calls without adding headcount."
4. Touch 1–2 pains relevant to {offer_summary.lower()}:
   - Human error from multi-tool chaos
   - No time to follow up properly
   - Leads slipping through the cracks
   - Manual, repetitive tasks blocking growth
5. Mention 1–2 practical outcomes:
   - Clear workflow map in 7 days
   - Automated follow-ups
   - Cleaner CRM and reporting
6. Clear CTA: invite them to a 15–20 min discovery call (free) to map quick wins; offer to send a 1‑page blueprint if they prefer async.
7. Keep tone human, specific, concise. No hype, no emojis, no square brackets.
8. Final sentence must include our company name: Outbound Accelerator.

### Output Format (MANDATORY):
Return **strictly valid JSON** with these keys:
{
  "subject": "<5–8 word subject line>",
  "body_html": "<HTML body: 1–2 short paragraphs and a one-line CTA in <p> tags>"
}
"""
    return prompt

def generate_email(lead):
    """
    Sends a prompt to OpenAI and returns the subject and body_html.
    """
    prompt = build_prompt(lead)

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a B2B cold email generator."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        content = response.choices[0].message.content

        try:
            email = json.loads(content)
        except Exception:
            email = eval(content.strip())
        return email

    except Exception as e:
        print(f"❌ Error generating email: {e}")
        return {"subject": "Follow up", "body_html": "Hi – just following up. Let me know if you're interested."}

def generate_email_from_prompt(prompt, openai_key):
    """
    Sends a custom prompt to OpenAI and returns the subject and body_html.
    """
    local_client = OpenAI(api_key=openai_key)

    try:
        print(f"📨 Prompt being sent:\n{prompt}")
        response = local_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a B2B cold email generator."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        content = response.choices[0].message.content

        try:
            email = json.loads(content)
        except Exception:
            print(f"⚠️ Raw OpenAI response content:\n{content}")
            email = {"subject": "Follow up", "body_html": content.strip()}

        return email

    except Exception as e:
        print(f"❌ Error generating email from prompt: {e}")
        return {"subject": "Follow up", "body_html": "Hi – just following up. Let me know if you're interested."}
