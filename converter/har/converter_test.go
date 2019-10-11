/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2017 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package har

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/loadimpact/k6/js"
	"github.com/loadimpact/k6/lib"
	"github.com/loadimpact/k6/lib/testutils/httpmultibin"
	"github.com/loadimpact/k6/loader"
	"github.com/loadimpact/k6/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildK6Headers(t *testing.T) {
	var headers = []struct {
		values   []Header
		expected []string
	}{
		{[]Header{{"name", "1"}, {"name", "2"}}, []string{`"name": "1"`}},
		{[]Header{{"name", "1"}, {"name2", "2"}}, []string{`"name": "1"`, `"name2": "2"`}},
		{[]Header{{":host", "localhost"}}, []string{}},
	}

	for _, pair := range headers {
		v := buildK6Headers(pair.values)
		assert.Equal(t, len(v), len(pair.expected), fmt.Sprintf("params: %v", pair.values))
	}
}

func TestBuildK6RequestObject(t *testing.T) {
	tb := httpmultibin.NewHTTPMultiBin(t)
	defer tb.Cleanup()
	sr := tb.Replacer.Replace

	t.Run("get", func(t *testing.T) {
		req := &Request{
			Method:  "get",
			URL:     sr("HTTPBIN_URL/get"),
			Headers: []Header{{"accept-language", "es-ES,es;q=0.8"}},
			Cookies: []Cookie{{Name: "a", Value: "b"}},
		}
		v, err := buildK6RequestObject(req)
		assert.NoError(t, err)
		_, err = js.New(&loader.SourceData{
			URL:  &url.URL{Path: "/script.js"},
			Data: []byte(fmt.Sprintf("export default function() { res = http.batch([%v]); }", v)),
		}, nil, lib.RuntimeOptions{})
		assert.NoError(t, err)
	})

	t.Run("post", func(t *testing.T) {
		req := &Request{
			Method:  "post",
			URL:     sr("HTTPBIN_URL/post"),
			Headers: []Header{{"accept-language", "es-ES,es;q=0.8"}},
			Cookies: []Cookie{{Name: "a", Value: "b"}},
			PostData: &PostData{
				Text:     "x\x01«VJIV²270ÕQª(V²240¨\x05\x002Ö\x05\x00",
				MimeType: "text/plain",
			},
		}
		v, err := buildK6RequestObject(req)
		require.NoError(t, err)
		r, err := js.New(&loader.SourceData{
			URL: &url.URL{Path: "/script.js"},
			Data: []byte(fmt.Sprintf(`
			import http from "k6/http";
			export default function() {
				let res = http.batch([%s]);
				if (res[0].status != 200) {
					throw new Error(JSON.stringify(res));
				}
			}`, v)),
		}, nil, lib.RuntimeOptions{})
		require.NoError(t, err)

		r.SetOptions(lib.Options{
			Hosts: tb.Dialer.Hosts,
		})

		ctx := context.Background()
		ch := make(chan<- stats.SampleContainer, 1000)
		vu, err := r.NewVU(ch)
		require.NoError(t, err)
		err = vu.RunOnce(ctx)
		require.NoError(t, err)
	})
}

func TestBuildK6Body(t *testing.T) {
	bodyText := "ccustemail=ppcano%40gmail.com&size=medium&topping=cheese&delivery=12%3A00&comments="

	req := &Request{
		Method: "post",
		URL:    "http://www.google.es",
		PostData: &PostData{
			MimeType: "application/x-www-form-urlencoded",
			Text:     bodyText,
		},
	}
	postParams, plainText, err := buildK6Body(req)
	assert.NoError(t, err)
	assert.Equal(t, len(postParams), 0, "postParams should be empty")
	assert.Equal(t, bodyText, plainText)

	email := "user@mail.es"
	expectedEmailParam := fmt.Sprintf(`"email": %q`, email)

	req = &Request{
		Method: "post",
		URL:    "http://www.google.es",
		PostData: &PostData{
			MimeType: "application/x-www-form-urlencoded",
			Params: []Param{
				{Name: "email", Value: url.QueryEscape(email)},
				{Name: "pw", Value: "hola"},
			},
		},
	}
	postParams, plainText, err = buildK6Body(req)
	assert.NoError(t, err)
	assert.Equal(t, plainText, "", "expected empty plainText")
	assert.Equal(t, len(postParams), 2, "postParams should have two items")
	assert.Equal(t, postParams[0], expectedEmailParam, "expected unescaped value")
}
