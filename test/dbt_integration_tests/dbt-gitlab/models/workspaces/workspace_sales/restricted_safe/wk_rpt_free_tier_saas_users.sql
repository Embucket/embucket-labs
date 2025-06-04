WITH dim_namespace AS (

  SELECT * FROM {{ ref('dim_namespace') }}

),

mart_product_usage_free_user_metrics_monthly AS (

  SELECT * FROM {{ ref('mart_product_usage_free_user_metrics_monthly') }}

),

gitlab_dotcom_users_xf AS (

  SELECT * FROM {{ ref('gitlab_dotcom_users_xf') }}

),

dim_marketing_contact_no_pii AS (

  SELECT * FROM {{ ref('dim_marketing_contact_no_pii') }}

),

mart_crm_account AS (

  SELECT * FROM {{ ref('mart_crm_account') }}
),

gitlab_dotcom_gitlab_subscriptions_snapshots AS (

  SELECT * FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_base') }}

),

active_namespaces AS (

  SELECT
    -- Base namespace information
    n.dim_namespace_id,
    n.namespace_name,
    n.created_at,
    n.creator_id,
    users.user_name                             AS creator_name,
    users.email                                 AS creator_email,
    SPLIT_PART(users.email, '@', 2)             AS domain,
    mc.dim_crm_account_id,
    a.crm_account_name,
    u.umau_28_days_user                         AS monthly_active_users,

    -- Additional Time-Based Raw Metrics
    DATEDIFF('day', n.created_at, CURRENT_DATE) AS namespace_age_days,

    -- 1. Core Development Workflow Metrics
    u.ci_builds_28_days_user,
    u.ci_pipelines_28_days_user,
    u.ci_pipeline_config_repository_28_days_user,
    u.commit_ci_config_file_28_days_user,
    u.source_code_pushes_all_time_event,
    u.web_ide_edit_28_days_user,

    -- 2. Collaboration Metrics
    u.code_review_user_approve_mr_28_days_user,
    u.protected_branches_28_days_user,
    u.merge_requests_with_required_code_owners_28_days_user,

    -- 3. Project Management Metrics
    u.project_management_issue_milestone_changed_28_days_user,
    u.project_management_issue_iteration_changed_28_days_user,
    u.issues_edit_28_days_user,
    u.service_desk_issues_28_days_user,

    -- 4. DevOps Practice Metrics
    u.auto_devops_pipelines_28_days_user,
    u.successful_deployments_28_days_user,
    u.failed_deployments_28_days_user,
    u.deployments_28_days_event,
    u.analytics_value_stream_28_days_event,
    u.ci_internal_pipelines_all_time_event,
    u.ci_external_pipelines_all_time_event,

    -- 5. Advanced Feature Usage
    u.packages_28_days_event,
    u.releases_28_days_user,
    u.feature_flags_all_time_event,
    u.analytics_28_days_user,

    -- 6. Security Practice Metrics
    u.user_sast_jobs_28_days_user,
    u.user_secret_detection_jobs_28_days_user,
    u.user_dependency_scanning_jobs_28_days_user,
    u.user_unique_users_all_secure_scanners_28_days_user,

    -- 7. Infrastructure Metrics
    u.project_clusters_enabled_28_days_user,
    u.active_project_runners_all_time_event,
    u.terraform_state_api_28_days_user,
    u.gitaly_servers_all_time_event

  FROM dim_namespace AS n
  INNER JOIN mart_product_usage_free_user_metrics_monthly AS u
    ON n.dim_namespace_id = u.dim_namespace_id
  LEFT JOIN gitlab_dotcom_users_xf AS users
    ON n.creator_id = users.user_id
  LEFT JOIN dim_marketing_contact_no_pii AS mc
    ON users.user_id = mc.gitlab_dotcom_user_id
  LEFT JOIN mart_crm_account AS a
    ON mc.dim_crm_account_id = a.dim_crm_account_id
  LEFT JOIN gitlab_dotcom_gitlab_subscriptions_snapshots AS subs
    ON n.dim_namespace_id = subs.namespace_id
      AND subs.valid_to IS NULL  -- Get current subscription record
  WHERE u.reporting_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
    AND n.is_setup_for_company = TRUE
    AND n.namespace_type = 'Group'
    AND u.delivery_type = 'SaaS'
    AND mc.dim_crm_account_id IS NOT NULL
    AND u.umau_28_days_user > 0

    AND (
      subs.namespace_id IS NULL  -- No subscription record
      OR subs.gitlab_subscription_end_date < CURRENT_DATE  -- Expired subscription
    )

    AND domain NOT IN (
      '123.com',
      '126.com',
      '139.com',
      '163.com',
      '189.cn',
      'abc.com',
      'adam.com.au',
      'address.com',
      'aim.com',
      'alice.it',
      'aliyun.com',
      'ameritech.net',
      'ancestry.com',
      'aol.com',
      'aon.at',
      'arcor.de',
      'asdf.com',
      'att.net',
      'bell.net',
      'bellsouth.net',
      'bigmir.net',
      'bigpond.com',
      'bigpond.net.au',
      'bk.ru',
      'blueyonder.co.uk',
      'bluemail.ch',
      'bol.com.br',
      'btconnect.com',
      'btinternet.com',
      'buzblox.com',
      'bvrithyderabad.edu.in',
      'cable.comcast.com',
      'caramail.com',
      'casema.nl',
      'centrum.cz',
      'chacuo.net',
      'charter.net',
      'chello.nl',
      'comcast.com',
      'comcast.net',
      'consultant.com',
      'cox.com',
      'cox.net',
      'cs.com',
      'cyberservices.com',
      'detik.com',
      'dr.com',
      'dxice.com',
      'earthcam.net',
      'earthlink.net',
      'eircom.net',
      'elisanet.fi',
      'em2lab.com',
      'email.com',
      'email.cz',
      'email.it',
      'eml.cc',
      'engineer.com',
      'europe.com',
      'eurosport.com',
      'facebook.com',
      'fastmail.com',
      'fastmail.fm',
      'fastmail.net',
      'foxmail.com',
      'free.fr',
      'freenet.de',
      'freemail.hu',
      'frontier.com',
      'funvane.com',
      'gazeta.pl',
      'getnada.com',
      'gmail.com',
      'gmx.at',
      'gmx.com',
      'gmx.de',
      'gmx.li',
      'gmx.net',
      'googlegroups.com',
      'googlemail.com',
      'hanmail.net',
      'hetnet.nl',
      'home.nl',
      'hotmail.ca',
      'hotmail.ch',
      'hotmail.co.jp',
      'hotmail.co.nz',
      'hotmail.co.uk',
      'hotmail.com',
      'hotmail.com.au',
      'hotmail.com.br',
      'hotmail.de',
      'hotmail.es',
      'hotmail.fi',
      'hotmail.fr',
      'hotmail.it',
      'hotmail.nl',
      'hushmail.com',
      'i.ua',
      'icloud.com',
      'iinet.net.au',
      'iname.com',
      'inbox.com',
      'inbox.ru',
      'internode.on.net',
      'interia.pl',
      'inwind.it',
      'juno.com',
      'kolumbus.fi',
      'korea.com',
      'kpnmail.nl',
      'laposte.net',
      'lenta.ru',
      'libero.it',
      'linuxmail.org',
      'list.ru',
      'live.at',
      'live.be',
      'live.ca',
      'live.cl',
      'live.cn',
      'live.co.uk',
      'live.co.za',
      'live.com',
      'live.com.ar',
      'live.com.au',
      'live.com.mx',
      'live.com.pt',
      'live.com.sg',
      'live.de',
      'live.dk',
      'live.fr',
      'live.ie',
      'live.in',
      'live.it',
      'live.jp',
      'live.nl',
      'live.no',
      'live.ru',
      'live.se',
      'mac.com',
      'mail.bg',
      'mail.com',
      'mail.de',
      'mail.ee',
      'mail.ru',
      'mail7.io',
      'maildrop.cc',
      'mailinator.com',
      'mchsi.com',
      'me.com',
      'mindspring.com',
      'mm.st',
      'msn.cn',
      'msn.com',
      'my.com',
      'mydomain.com',
      'myself.com',
      'name.com',
      'naseej.com',
      'naver.com',
      'nc.rr.com',
      'netc.fr',
      'netcourrier.com',
      'nespressopixie.com',
      'netscape.net',
      'netspace.net.au',
      'netzero.net',
      'ntlworld.com',
      'null.net',
      'nus.edu.sg',
      'oath.com',
      'online.nl',
      'onet.eu',
      'onet.pl',
      'op.pl',
      'optonline.net',
      'optusnet.com.au',
      'orange.fr',
      'outlook.at',
      'outlook.be',
      'outlook.cl',
      'outlook.co.id',
      'outlook.co.il',
      'outlook.co.nz',
      'outlook.co.th',
      'outlook.com',
      'outlook.com.au',
      'outlook.com.br',
      'outlook.com.tr',
      'outlook.com.vn',
      'outlook.cz',
      'outlook.de',
      'outlook.dk',
      'outlook.es',
      'outlook.hu',
      'outlook.ie',
      'outlook.in',
      'outlook.it',
      'outlook.jp',
      'outlook.kr',
      'outlook.my',
      'outlook.ph',
      'outlook.pt',
      'outlook.sa',
      'outlook.sg',
      'ozemail.com.au',
      'pacbell.net',
      'phantom-mail.io',
      'planet.nl',
      'poczta.fm',
      'poczta.onet.pl',
      'post.com',
      'post.cz',
      'prevcure.com',
      'programmer.net',
      'proton.me',
      'protonmail.com',
      'ptd.net',
      'q.com',
      'quantentunnel.de',
      'quicknet.nl',
      'rambler.ru',
      'rcn.com',
      'rediff.com',
      'rediffmail.com',
      'ro.ru',
      'roadrunner.com',
      'rocketmail.com',
      'rogers.com',
      'runbox.com',
      'sapo.pt',
      'saudia.com',
      'sbcglobal.net',
      'sdf.com',
      'sent.com',
      'seznam.cz',
      'sharklasers.com',
      'shaw.ca',
      'sina.cn',
      'sina.com',
      'singnet.com.sg',
      'sky.com',
      'skynet.be',
      'snkmail.com',
      'sohu.com',
      'spoko.pl',
      'sudomail.com',
      'swbell.net',
      't-online.de',
      'talk21.com',
      'tech-center.com',
      'techie.com',
      'technologist.com',
      'telefonica.net',
      'telenet.be',
      'telfort.nl',
      'telstra.com',
      'telstra.com.au',
      'terra.com.br',
      'test.com',
      'thraml.com',
      'tiscali.co.uk',
      'tiscali.it',
      'tom.com',
      'tpg.com.au',
      'trash-mail.com',
      'tut.by',
      'twc.com',
      'tx.rr.com',
      'ua.fm',
      'ukr.net',
      'unitybox.de',
      'uol.com.br',
      'upcmail.nl',
      'usa.com',
      'usa.net',
      'verizon.net',
      'videotron.ca',
      'vip.qq.com',
      'virgin.net',
      'virginmedia.com',
      'virgilio.it',
      'vp.pl',
      'wanadoo.fr',
      'web.de',
      'windowslive.com',
      'workmail.com',
      'worldnet.att.net',
      'wp.pl',
      'xtra.co.nz',
      'xs4all.nl',
      'y7mail.com',
      'ya.ru',
      'yahoo.ca',
      'yahoo.co.id',
      'yahoo.co.in',
      'yahoo.co.jp',
      'yahoo.co.kr',
      'yahoo.co.nz',
      'yahoo.co.uk',
      'yahoo.com',
      'yahoo.com.ar',
      'yahoo.com.au',
      'yahoo.com.br',
      'yahoo.com.hk',
      'yahoo.com.mx',
      'yahoo.com.ph',
      'yahoo.com.sg',
      'yahoo.com.tw',
      'yahoo.com.vn',
      'yahoo.de',
      'yahoo.dk',
      'yahoo.es',
      'yahoo.fr',
      'yahoo.gr',
      'yahoo.ie',
      'yahoo.in',
      'yahoo.it',
      'yahoo.no',
      'yahoo.pl',
      'yahoo.ro',
      'yahoo.se',
      'yandex.com',
      'yandex.ru',
      'yeah.net',
      'ymail.com',
      'yopmail.com',
      'ziggo.nl',
      'zoho.com'
    )
)

SELECT
  -- Original fields from the CTE
  *,

  -- Core Development Score Components (40 pts max)
  CASE WHEN ci_pipelines_28_days_user > 0 THEN 10 ELSE 0 END                          AS ci_pipeline_score,
  CASE
    WHEN ci_builds_28_days_user > 10 THEN 10
    WHEN ci_builds_28_days_user > 0 THEN 5
    ELSE 0
  END                                                                                 AS ci_builds_score,
  CASE
    WHEN deployments_28_days_event > 50 THEN 20
    WHEN deployments_28_days_event > 10 THEN 15
    WHEN deployments_28_days_event > 0 THEN 10
    ELSE 0
  END                                                                                 AS deployment_score,

  -- Collaboration Score Components (30 pts max)
  CASE WHEN code_review_user_approve_mr_28_days_user > 0 THEN 10 ELSE 0 END           AS code_review_score,
  CASE WHEN protected_branches_28_days_user > 0 THEN 10 ELSE 0 END                    AS protected_branches_score,
  CASE WHEN issues_edit_28_days_user > 0 THEN 10 ELSE 0 END                           AS issue_tracking_score,

  -- Advanced Features Score Components (30 pts max)
  CASE WHEN user_unique_users_all_secure_scanners_28_days_user > 0 THEN 10 ELSE 0 END AS security_score,
  CASE WHEN analytics_28_days_user > 0 THEN 10 ELSE 0 END                             AS analytics_score,
  CASE WHEN feature_flags_all_time_event > 0 THEN 10 ELSE 0 END                       AS feature_flags_score,

  -- Category Subtotals
  LEAST(
    40,
    CASE WHEN ci_pipelines_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN ci_builds_28_days_user > 10 THEN 10
      WHEN ci_builds_28_days_user > 0 THEN 5
      ELSE 0
    END
    + CASE WHEN deployments_28_days_event > 50 THEN 20
      WHEN deployments_28_days_event > 10 THEN 15
      WHEN deployments_28_days_event > 0 THEN 10
      ELSE 0
    END
  )                                                                                   AS core_development_score,

  LEAST(
    30,
    CASE WHEN code_review_user_approve_mr_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN protected_branches_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN issues_edit_28_days_user > 0 THEN 10 ELSE 0 END
  )                                                                                   AS collaboration_score,

  LEAST(
    30,
    CASE WHEN user_unique_users_all_secure_scanners_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN analytics_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN feature_flags_all_time_event > 0 THEN 10 ELSE 0 END
  )                                                                                   AS advanced_features_score,

  -- Total Activity Score
  LEAST(
    40,
    CASE WHEN ci_pipelines_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN ci_builds_28_days_user > 10 THEN 10
      WHEN ci_builds_28_days_user > 0 THEN 5
      ELSE 0
    END
    + CASE WHEN deployments_28_days_event > 50 THEN 20
      WHEN deployments_28_days_event > 10 THEN 15
      WHEN deployments_28_days_event > 0 THEN 10
      ELSE 0
    END
  )
  + LEAST(
    30,
    CASE WHEN code_review_user_approve_mr_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN protected_branches_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN issues_edit_28_days_user > 0 THEN 10 ELSE 0 END
  )
  + LEAST(
    30,
    CASE WHEN user_unique_users_all_secure_scanners_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN analytics_28_days_user > 0 THEN 10 ELSE 0 END
    + CASE WHEN feature_flags_all_time_event > 0 THEN 10 ELSE 0 END
  )                                                                                   AS total_activity_score,

  -- Activity Level
  CASE
    WHEN (
      LEAST(
        40,
        CASE WHEN ci_pipelines_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN ci_builds_28_days_user > 10 THEN 10
          WHEN ci_builds_28_days_user > 0 THEN 5
          ELSE 0
        END
        + CASE WHEN deployments_28_days_event > 50 THEN 20
          WHEN deployments_28_days_event > 10 THEN 15
          WHEN deployments_28_days_event > 0 THEN 10
          ELSE 0
        END
      )
      + LEAST(
        30,
        CASE WHEN code_review_user_approve_mr_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN protected_branches_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN issues_edit_28_days_user > 0 THEN 10 ELSE 0 END
      )
      + LEAST(
        30,
        CASE WHEN user_unique_users_all_secure_scanners_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN analytics_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN feature_flags_all_time_event > 0 THEN 10 ELSE 0 END
      )
    ) >= 40 THEN 'High Activity'
    WHEN (
      LEAST(
        40,
        CASE WHEN ci_pipelines_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN ci_builds_28_days_user > 10 THEN 10
          WHEN ci_builds_28_days_user > 0 THEN 5
          ELSE 0
        END
        + CASE WHEN deployments_28_days_event > 50 THEN 20
          WHEN deployments_28_days_event > 10 THEN 15
          WHEN deployments_28_days_event > 0 THEN 10
          ELSE 0
        END
      )
      + LEAST(
        30,
        CASE WHEN code_review_user_approve_mr_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN protected_branches_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN issues_edit_28_days_user > 0 THEN 10 ELSE 0 END
      )
      + LEAST(
        30,
        CASE WHEN user_unique_users_all_secure_scanners_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN analytics_28_days_user > 0 THEN 10 ELSE 0 END
        + CASE WHEN feature_flags_all_time_event > 0 THEN 10 ELSE 0 END
      )
    ) >= 15 THEN 'Medium Activity'
    ELSE 'Low Activity'
  END                                                                                 AS activity_level,

  COALESCE(
    -- Core Development Usage
    CASE
      WHEN core_development_score >= 30
        THEN 'This namespace demonstrates intensive development activity with extensive CI/CD usage and deployment practices. '
      WHEN core_development_score >= 15
        THEN 'This namespace shows moderate development activity with regular CI/CD pipeline usage. '
      WHEN core_development_score > 0
        THEN 'This namespace has basic development activity with minimal CI/CD usage. '
      ELSE ''
    END

    -- Deployment Volume
    || CASE
      WHEN deployments_28_days_event > 50
        THEN 'They are running a high-volume deployment environment with over 50 deployments per month, indicating mature DevOps practices. '
      WHEN deployments_28_days_event > 10
        THEN 'They maintain regular deployment activity with 10-50 deployments monthly, showing growing DevOps maturity. '
      WHEN deployments_28_days_event > 0
        THEN 'They have started implementing deployment practices with occasional deployments. '
      ELSE ''
    END

    -- Collaboration Patterns
    || CASE
      WHEN collaboration_score >= 20
        THEN 'The team shows strong collaboration practices, actively using code reviews, protected branches, and issue tracking. '
      WHEN collaboration_score >= 10
        THEN 'There is moderate team collaboration with some usage of GitLab''s collaboration features. '
      WHEN collaboration_score > 0
        THEN 'The team is beginning to adopt collaboration features. '
      ELSE ''
    END

    -- User Activity Scale
    || CASE
      WHEN monthly_active_users > 10
        THEN 'This is a sizeable team with over 10 active users engaging with the platform monthly. '
      WHEN monthly_active_users > 5
        THEN 'This is a medium-sized team with 5-10 active users. '
      ELSE 'This is a small team with less than 5 active users. '
    END

    -- Advanced Features - Security
    || CASE
      WHEN user_sast_jobs_28_days_user > 0
        THEN 'The team has implemented SAST scanning for code security. '
      ELSE ''
    END

    || CASE
      WHEN user_secret_detection_jobs_28_days_user > 0
        THEN 'They are actively using secret detection to prevent credential exposure. '
      ELSE ''
    END

    || CASE
      WHEN user_dependency_scanning_jobs_28_days_user > 0
        THEN 'Dependency scanning is in place to monitor third-party vulnerabilities. '
      ELSE ''
    END

    -- Advanced Features - Analytics & Planning
    || CASE
      WHEN analytics_28_days_user > 0
        THEN 'The team utilizes GitLab analytics for project insights. '
      ELSE ''
    END

    || CASE
      WHEN analytics_value_stream_28_days_event > 0
        THEN 'They are leveraging value stream analytics to optimize their development workflow. '
      ELSE ''
    END

    -- Advanced Features - Infrastructure
    || CASE
      WHEN project_clusters_enabled_28_days_user > 0
        THEN 'They have configured Kubernetes clusters for deployment infrastructure. '
      ELSE ''
    END

    || CASE
      WHEN terraform_state_api_28_days_user > 0
        THEN 'The team is managing infrastructure as code using Terraform integration. '
      ELSE ''
    END

    -- Advanced Features - Release Management
    || CASE
      WHEN feature_flags_all_time_event > 0
        THEN 'Feature flags are being used for controlled feature rollouts. '
      ELSE ''
    END

    || CASE
      WHEN packages_28_days_event > 0
        THEN 'They are utilizing GitLab package registry for artifact management. '
      ELSE ''
    END

    || CASE
      WHEN releases_28_days_user > 0
        THEN 'The team is managing releases through GitLab''s release features. '
      ELSE ''
    END

    -- Activity Level Summary
    || CASE
      WHEN activity_level = 'High Activity'
        THEN 'Overall, this is a highly active namespace that would benefit from additional Enterprise features and support.'
      WHEN activity_level = 'Medium Activity'
        THEN 'Overall, this namespace shows promising activity levels and could benefit from additional GitLab features.'
      ELSE 'Overall, this namespace has opportunity for increased GitLab adoption and usage.'
    END,
    'Limited usage data available for this namespace.'
  )                                                                                   AS usage_summary

FROM active_namespaces
ORDER BY total_activity_score DESC
