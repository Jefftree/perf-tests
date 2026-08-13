# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO off so the binary runs on distroless static.
RUN CGO_ENABLED=0 go build -o /out/watch-fanout .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/watch-fanout /watch-fanout
USER 65532:65532
ENTRYPOINT ["/watch-fanout"]
