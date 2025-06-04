## Source

{% docs zengrc_desc %}
NYI
{% enddocs %}

## Tables
{% docs zengrc_assessments_desc %}
Objects used to assess the effectiveness of a control. Assessments are typically made after requested evidence has been submitted and based on that evidence. Assessments are made on the 1) Design and the 2) Operation of a control by selecting either “Effective” or “Ineffective." Typically, controls that receive an “Ineffective” rating in either category will have a corresponding issue created. The status of assessment objects is tracked in Audits.  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_audits_desc %}
A container object for audits run against controls. This object will contain metadata around the audit itself (i.e., audit title, audit period, audit managers, etc.). When creating an audit, any requests and assessments will be automatically mapped to the audit object. Additionally, Issues created from assessments will be mapped to the corresponding audit object.  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_controls_desc %}
Prescriptive guidelines or rules set in place to ensure a company meets its compliance goals. Often they are step-by-step instructions or commands that when met, assure compliance. We define controls as a company solution that mitigates risks and supports the compliance of its mapped objective. Controls are the only objects that are tested in the audit module in ZenGRC. They can be mapped to various objects to allow audit flexibility (i.e., controls mapped to system A to allow an audit of system A).  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_objectives_desc %}
An individual compliance objective, or requirement that must be met by the organization. This is the actual verbiage of the requirement from the authoritative source document and what must be satisfied by organizational controls to achieve compliance (i.e., Requirement 1.1.1 of the PCI-DSS). Because they are quite vague, interpretation of objectives can vary by company and more actionable, specific controls are often put in place to ensure that objectives are met. We define an objective as an actionable goal that serves to uphold a compliance requirement (the opposite of a risk).  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_issues_desc %}
A gap or finding that requires remediation or acceptance. It is used to track the remediation of an issue (finding) discovered during the assessment of a control. Information that this object should contain includes an owner, remediation plan, and due date. The status of Issue objects is tracked in Audits.  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_requests_desc %}
Objects used to request evidence as part of an assessment. The request object is sent to the identified assignee, who can respond to the request and upload evidence. Additionally, all communication between the assessor and the assignee will be tracked. Request status is tracked in Audits.  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_risks_desc %}
Objects used to track risks to the organization. Risks are identified dangers that could potentially harm the organization if not addressed. By mapping relevant controls to identified risks, an organization can identify controls that have been put in place to minimize risk. This is commonly known as a Risk and Control Matrix (RCM).  [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)
{% enddocs %}

{% docs zengrc_programs_desc %}

Programs are typically standardized, industry-wide compliance guidelines issued by large authoritative sources. In ZenGRC, a program contains all objects related to one authoritative source. It can be a regulatory framework (NIST 800-53, PCI, etc.) or a custom program to manage a compliance initiative (Enterprise Risk Management, Vendor Management, etc.). It serves as a “container” for other compliance objects, and can also be mapped to other objects (including non-compliance objects) to indicate a requirement for regulatory compliance. [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)

{% enddocs %}

{% docs zengrc_vendors_desc %}

An entity that provides products or services to the organization. To use the Vendor Risk Management module, vendor objects must be created. Can be used to manage vendor compliance to specific controls and/or objectives (requirements). Can also be set as an audit target by mapping relevant controls. [Source Documentation](https://reciprocitylabs.atlassian.net/wiki/spaces/ZenGRCOnboardingGuide/pages/42139682/ZenGRC+Definitions)

{% enddocs %}
