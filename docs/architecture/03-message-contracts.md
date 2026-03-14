# Message Contracts

Public handler surface (read): GetSiteByHost, ResolveRoute, GetPage, GetLayout, GetNavigation, GetProduct, ListCategoryProducts, HasEntitlement.

Public handler surface (write): RegisterSite, BindDomain, PutDraft, UpsertRoute, PublishVersion, UpsertProduct, GrantRole, GrantEntitlement.

Standard tags to include on every message: Action, Site-Id, Version, Locale, Request-Id, Actor-Role, Schema-Version, Publish-Id, Nonce, Signature-Ref.
