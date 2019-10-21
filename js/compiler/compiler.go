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

//go:generate rice embed-go

package compiler

import (
	"sync"
	"time"

	rice "github.com/GeertJohan/go.rice"
	"github.com/dop251/goja"
	"github.com/dop251/goja/parser"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
)

var (
	DefaultOpts = map[string]interface{}{
		"presets":       []string{"latest"},
		"ast":           false,
		"sourceMaps":    false,
		"babelrc":       false,
		"compact":       false,
		"retainLines":   true,
		"highlightCode": false,
	}

	once sync.Once // nolint:gochecknoglobals
	babl *babel    // nolint:gochecknoglobals
)

// CompatibilityMode specifies the JS compatibility mode
// nolint:lll
//go:generate enumer -type=CompatibilityMode -transform=snake -trimprefix CompatibilityMode -output compatibility_mode_gen.go
type CompatibilityMode uint8

const (
	// CompatibilityModeES6 is achieved with Babel and core.js
	CompatibilityModeES6 CompatibilityMode = iota + 1
	// CompatibilityModeES51 is standard goja
	CompatibilityModeES51
)

// A Compiler compiles JavaScript source code (ES5.1 or ES6) into a goja.Program
type Compiler struct{}

// New returns a new Compiler
func New() *Compiler {
	return &Compiler{}
}

// Transform the given code into ES5, while synchronizing to ensure only a single
// babel instance is in use.
func (c *Compiler) Transform(src, filename string) (code string, srcmap SourceMap, err error) {
	var b *babel
	if b, err = newBabel(); err != nil {
		return
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.Transform(src, filename)
}

// Compile the program
func (c *Compiler) Compile(src, filename string, pre, post string,
	strict bool, compatMode CompatibilityMode) (*goja.Program, string, error) {
	return c.compile(src, filename, pre, post, strict, compatMode)
}

func (c *Compiler) compile(src, filename string, pre, post string,
	strict bool, compatMode CompatibilityMode) (*goja.Program, string, error) {
	code := pre + src + post
	ast, err := parser.ParseFile(nil, filename, code, 0)
	if err != nil {
		if compatMode == CompatibilityModeES6 {
			logrus.Debug("Compiling to ES5")
			code, _, err := c.Transform(src, filename)
			if err != nil {
				return nil, code, err
			}
			return c.compile(code, filename, pre, post, strict, compatMode)
		}
		return nil, src, err
	}
	pgm, err := goja.CompileAST(ast, strict)
	return pgm, code, err
}

type babel struct {
	vm        *goja.Runtime
	this      goja.Value
	transform goja.Callable
	opts      map[string]interface{}
	mutex     sync.Mutex //TODO: cache goja.CompileAST() in an init() function?
}

func newBabel() (*babel, error) {
	var err error

	once.Do(func() {
		conf := rice.Config{
			LocateOrder: []rice.LocateMethod{rice.LocateEmbedded},
		}
		babelSrc := conf.MustFindBox("lib").MustString("babel.min.js")
		vm := goja.New()
		if _, err = vm.RunString(babelSrc); err != nil {
			return
		}
		opts := make(map[string]interface{})
		for k, v := range DefaultOpts {
			opts[k] = v
		}

		this := vm.Get("Babel")
		bObj := this.ToObject(vm)
		babl = &babel{vm: vm, this: this, opts: opts}
		if err = vm.ExportTo(bObj.Get("transform"), &babl.transform); err != nil {
			return
		}
	})

	return babl, err
}

func (b *babel) Transform(src, filename string) (code string, srcmap SourceMap, err error) {
	opts := b.opts
	opts["filename"] = filename

	startTime := time.Now()
	v, err := b.transform(b.this, b.vm.ToValue(src), b.vm.ToValue(opts))
	if err != nil {
		return
	}
	logrus.WithField("t", time.Since(startTime)).Debug("Babel: Transformed")

	vO := v.ToObject(b.vm)
	if err = b.vm.ExportTo(vO.Get("code"), &code); err != nil {
		return
	}
	var rawmap map[string]interface{}
	if err = b.vm.ExportTo(vO.Get("map"), &rawmap); err != nil {
		return
	}
	if err = mapstructure.Decode(rawmap, &srcmap); err != nil {
		return
	}
	return
}
