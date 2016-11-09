
(require '[arachne.http.dsl :as http])
(require '[arachne.chimera.dsl :as m])
(require '[arachne.chimera.jdbc.dsl :as jdbc])

(load-file "blog-migrations.clj")

(jdbc/adapter :my.app/primary-db
  :driver org.postgresql.Driver
  :url "jdbc:postgresql://<HOST>:<PORT>/<DB>"
  :username "dbuser"
  :password "12345"
  :migrations [:my.app.migrations/add-tags])

(http/handler :my.app/index {:my.app/primary-db :db}
  'my.app.blog/index-handler)
