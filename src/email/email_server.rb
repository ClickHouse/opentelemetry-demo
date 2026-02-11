# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

require "ostruct"
require "pony"
require "sinatra"

set :port, ENV["EMAIL_PORT"]

post "/send_order_confirmation" do
  data = JSON.parse(request.body.read, object_class: OpenStruct)

  send_email(data)

end

error do
  # Error handling without OTEL
  halt 500
end

def send_email(data)
  Pony.mail(
    to:       data.email,
    from:     "noreply@example.com",
    subject:  "Your confirmation email",
    body:     erb(:confirmation, locals: { order: data.order }),
    via:      :test
  )
  puts "Order confirmation email sent to: #{data.email}"
end
