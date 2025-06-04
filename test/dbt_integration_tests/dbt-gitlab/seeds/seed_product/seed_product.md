{% docs gitlab_dotcom_access_levels %}

The names and meanings for the access levels are taken directly from the GitLab code and API reference documentation

https://gitlab.com/gitlab-org/gitlab/-/blob/master/lib/gitlab/access.rb#L12

```ruby
    NO_ACCESS      = 0
    MINIMAL_ACCESS = 5
    GUEST          = 10
    REPORTER       = 20
    DEVELOPER      = 30
    MAINTAINER     = 40
    OWNER          = 50
```

https://docs.gitlab.com/ee/api/access_requests.html

{% enddocs %}