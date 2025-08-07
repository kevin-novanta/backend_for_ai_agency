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
Write a short, warm cold outreach email (under 120 words) to the company above.

### Script Rules:
1. Do NOT summarize what the company does. Instead, use their offer to show deep understanding and context.
2. Mention our company — Outbound Accelerator — as specialists in advanced AI workflows.
3. Say: "We help companies like yours — companies that {offer_summary.lower()} — streamline complex operations and drive results."
4. Mention tailored-specific results: reduce manual tasks, eliminate inefficiencies, and increase booked calls by 2–3x. (This will not be in the email, but is for your context.)
5. Also include 1–2 of these 2025-relevant benefits of AI:
   - Smarter, faster decisions
   - Scaling without hiring
   - Personalized experiences
   - Automated compliance/risk tracking
   - Refocusing team on high-value work
6. Also highlight 1–2 common pain points solved:
   - Human error from messy workflows
   - Tool overload / siloed systems
   - Workload overload (e.g., 1 person juggling 50+ clients)
   - No automation or optimization support
   - Old systems blocking full-funnel automation
7. Be specific with industry context: instead of “businesses like yours,” say “other {industry_info.lower()} businesses.”
8. Close by naming a likely pain (complexity, error, etc.) and why a tailored workflow would help.
9. End the email inviting them to watch a short video breakdown showing how our system works and how it applies to them.
10. Final sentence must include our company name: Outbound Accelerator.
11. This email must be short, warm, and human-sounding. Avoid sounding generic or robotic. 

### Example Output (for style/tone only):
"At Outbound Accelerator, we help online learning companies reduce backend tasks and book more calls with AI-powered workflows tailored to their process."

### Output Format:
- Keep it casual, friendly, human-sounding
- No emojis, fake flattery, square brackets, or robotic tone
"""

# 🟡 Follow-Up Email Template Prompt
follow_up_prompt_template = """
You already sent a cold email to this business but received no reply. Write a follow-up email that:

- Feels casual and helpful (not pushy)
- Reminds them briefly who we are and what we offer
- Reframes the opportunity in a slightly different way
- Ends with a soft CTA (e.g., "Let me know if you'd like me to send over a quick overview.")

# Company Name:
{company_name}

# Industry / Category:
{category}

# Business Summary:
{offer_summary}
"""

# 📝 Reengagement Email Template Prompt
reengagement_prompt_template = """
Write a re-engagement email for a lead who previously showed interest but didn’t book a call. The email should:

- Feel thoughtful and human
- Reference that we had previously connected
- Offer to revisit the opportunity or answer questions
- Be short, warm, and low-pressure

# Company Name:
{company_name}

# Industry / Category:
{category}

# Business Summary:
{offer_summary}
"""

def get_opener_prompt(company_name, category, offer_summary):
    return cold_email_prompt_template.format(
        company_name=company_name,
        category=category,
        offer_summary=offer_summary
    )

def get_follow_up_prompt(company_name, category, offer_summary):
    return follow_up_prompt_template.format(
        company_name=company_name,
        category=category,
        offer_summary=offer_summary
    )

def get_reengagement_prompt(company_name, category, offer_summary):
    return reengagement_prompt_template.format(
        company_name=company_name,
        category=category,
        offer_summary=offer_summary
    )