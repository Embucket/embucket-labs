{% docs zendesk_ticket_sla %}

This table calculates SLA status information for each ticket at the time of first reply. For the purpose of determining whether an SLA policy target was met or not Zendesk [calculates](https://support.zendesk.com/hc/en-us/articles/205951808-Understanding-first-reply-time-Professional-and-Enterprise-#topic_hxr_pqd_1hb) first reply as the minimum value between first reply by an agent, first resolution time, and full resolution time.

Furthermore SLA policies and ticket priorities can be changed, either by an automatic trigger or manual agent action, at any point prior to or after the first reply. In order to calculate whether an SLA policy target was met the most recent SLA policy and ticket priority prior to first reply are used according to the ticket [audit log](https://www.stitchdata.com/docs/integrations/saas/zendesk#ticket-audits).

The operating metric can be found [here](https://about.gitlab.com/handbook/finance/operating-metrics/#service-level-agreement-sla). SLA's can be found [here](https://about.gitlab.com/support/).

{% enddocs %}
