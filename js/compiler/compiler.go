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

	compilerInstance *Compiler
	once             sync.Once
)

// CompatibilityMode specifies the JS compatibility mode
// nolint:lll
//go:generate enumer -type=CompatibilityMode -transform=snake -trimprefix CompatibilityMode -output compatibility_mode_gen.go
type CompatibilityMode uint

// "es6": default, Goja+Babel+core.js
// "es51": plain Goja
const (
	CompatibilityModeES6 CompatibilityMode = iota
	CompatibilityModeES51
)

// A Compiler uses Babel to compile ES6 code into something ES5-compatible.
type Compiler struct {
	vm *goja.Runtime

	// JS pointers.
	this              goja.Value
	transform         goja.Callable
	mutex             sync.Mutex //TODO: cache goja.CompileAST() in an init() function?
	compatibilityMode CompatibilityMode
}

// New constructs a new compiler with the provided compatibility mode
func New(compatMode CompatibilityMode) (*Compiler, error) {
	var err error
	once.Do(func() {
		compilerInstance, err = new(compatMode)
	})

	return compilerInstance, err
}

func new(compatMode CompatibilityMode) (*Compiler, error) {
	conf := rice.Config{
		LocateOrder: []rice.LocateMethod{rice.LocateEmbedded},
	}

	c := &Compiler{vm: goja.New(), compatibilityMode: compatMode}
	logrus.WithField("compatibilityMode", compatMode).Debug("Created JS compiler")

	if compatMode == CompatibilityModeES6 {
		babelSrc := conf.MustFindBox("lib").MustString("babel.min.js")
		if _, err := c.vm.RunString(babelSrc); err != nil {
			return nil, err
		}

		c.this = c.vm.Get("Babel")
		thisObj := c.this.ToObject(c.vm)
		if err := c.vm.ExportTo(thisObj.Get("transform"), &c.transform); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Transform the given code into ES5.
func (c *Compiler) Transform(src, filename string) (code string, srcmap SourceMap, err error) {
	opts := make(map[string]interface{})
	for k, v := range DefaultOpts {
		opts[k] = v
	}
	opts["filename"] = filename

	c.mutex.Lock()
	defer c.mutex.Unlock()

	startTime := time.Now()
	v, err := c.transform(c.this, c.vm.ToValue(src), c.vm.ToValue(opts))
	if err != nil {
		return code, srcmap, err
	}
	logrus.WithField("t", time.Since(startTime)).Debug("Babel: Transformed")
	vO := v.ToObject(c.vm)

	if err := c.vm.ExportTo(vO.Get("code"), &code); err != nil {
		return code, srcmap, err
	}

	var rawmap map[string]interface{}
	if err := c.vm.ExportTo(vO.Get("map"), &rawmap); err != nil {
		return code, srcmap, err
	}
	if err := mapstructure.Decode(rawmap, &srcmap); err != nil {
		return code, srcmap, err
	}

	return code, srcmap, nil
}

// Compile the program
func (c *Compiler) Compile(src, filename string, pre, post string, strict bool) (*goja.Program, string, error) {
	return c.compile(src, filename, pre, post, strict)
}

func (c *Compiler) compile(src, filename string, pre, post string, strict bool) (*goja.Program, string, error) {
	code := pre + src + post
	ast, err := parser.ParseFile(nil, filename, code, 0)
	if err != nil {
		if c.compatibilityMode == CompatibilityModeES6 {
			code, _, err := c.Transform(src, filename)
			if err != nil {
				return nil, code, err
			}
			return c.compile(code, filename, pre, post, strict)
		}
		return nil, src, err
	}
	pgm, err := goja.CompileAST(ast, strict)
	return pgm, code, err
}
