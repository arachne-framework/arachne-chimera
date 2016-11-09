
(require '[arachne.chimera.dsl :as m])

(m/migration :my.app.migrations/initial-setup [:arachne.auth/identities]

  (m/create-attr :my.app/Content :my.app.content/author :ref :arachne.auth/Identity :min 1)
  (m/create-attr :my.app/Content :my.app.content/date :instant :min 1 :max 1)

  (m/extend-type :my.app/BlogPost :my.app/Content)

  (m/create-attr :my.app/BlogPost :my.app.blog-post/title :string :min 1 :max 1)
  (m/create-attr :my.app/BlogPost :my.app.blog-post/body :string :min 1 :max 1)

  )


(m/migration :my.app.migrations/add-tags [:my.app.migrations/initial-setup]
  (m/create-attr :my.app/Content :my.app.content/tags :string :min 0)
  )

