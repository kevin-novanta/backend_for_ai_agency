import pandas as pd

# Optional: Central reference for CRM leads path
CRM_LEADS_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads_copy.csv"

# ============================================
# 📧 Email Scripting Templates and GPT Prompts
# ============================================

# 🟢 Cold Email (Opener) Template Prompt
cold_email_prompt_template = """
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

### Output Format:
- Casual, friendly, human
- 1–2 short paragraphs + a one‑line CTA
"""

# 🟡 Follow-Up Email Template Prompt
follow_up_prompt_template = """
You already sent a cold email to this business but received no reply. Write a follow-up email that:

- Feels casual and helpful (not pushy)
- Reminds them we offer a free 15–20 min discovery call (workflow audit)
- Reframes value with one concrete outcome (e.g., automate follow-ups, de-duplicate leads, unify reporting)
- Offers an async option: "I can send a 1‑page blueprint if you prefer."
- Ends with a soft CTA: "Worth a quick look?"

# Company Name:
{company_name}

# Industry / Category:
{industry_info}

# Business Summary / Pains:
{offer_summary}

# Notes:
- Mention Outbound Accelerator once.
- Keep under 90 words.
- Human tone; no emojis or fluff.
"""

# 📝 Reengagement Email Template Prompt
reengagement_prompt_template = """
Write a re‑engagement email for a lead who previously showed interest but didn’t book. The email should:

- Acknowledge prior convo briefly (no guilt)
- Highlight one improvement since then (e.g., tighter lead routing, smarter follow-ups, clearer dashboard)
- Offer a free 15–20 min discovery call or a 1‑page blueprint recap
- Stay warm, low‑pressure, and specific to their context

# Company Name:
{company_name}

# Industry / Category:
{industry_info}

# Business Summary / Pains:
{offer_summary}

# Requirements:
- 70–100 words
- End with a soft question ("want me to send the 1‑pager?")
- Mention Outbound Accelerator once.
"""


def load_leads_from_csv(csv_path):
    return pd.read_csv(csv_path)

def get_opener_prompt(row):
    return cold_email_prompt_template.format(
        company_name=row['Lead Name'],
        industry_info=row['Industry'] if 'Industry' in row else row['Offer Type'],
        offer_summary=row['Main Pain Points']
    )

def get_follow_up_prompt(row):
    return follow_up_prompt_template.format(
        company_name=row['Lead Name'],
        industry_info=row['Industry'] if 'Industry' in row else row['Offer Type'],
        offer_summary=row['Main Pain Points']
    )

def get_reengagement_prompt(row):
    return reengagement_prompt_template.format(
        company_name=row['Lead Name'],
        industry_info=row['Industry'] if 'Industry' in row else row['Offer Type'],
        offer_summary=row['Main Pain Points']
    )