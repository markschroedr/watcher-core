package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/invopop/jsonschema"
	validation "github.com/santhosh-tekuri/jsonschema/v6"
)

// pd-memory's schema and decoding machinery, kept at the CLI boundary.
func Schema(v any) map[string]any {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	r := jsonschema.Reflector{DoNotReference: true, ExpandedStruct: t.Kind() == reflect.Struct}
	b, _ := json.Marshal(r.Reflect(v))
	var s map[string]any
	json.Unmarshal(b, &s)
	var normalize func(any)
	normalize = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			delete(node, "$schema")
			delete(node, "$id")
			if variants, ok := node["oneOf"]; ok {
				node["anyOf"] = variants
				delete(node, "oneOf")
			}
			for _, child := range node {
				normalize(child)
			}
		case []any:
			for _, child := range node {
				normalize(child)
			}
		}
	}
	normalize(s)
	return s
}
func ValidateJSON(data []byte, schema map[string]any) error {
	var value any
	if e := json.Unmarshal(data, &value); e != nil {
		return e
	}
	compiler := validation.NewCompiler()
	const location = "https://watcher.invalid/input"
	if e := compiler.AddResource(location, schema); e != nil {
		return e
	}
	compiled, e := compiler.Compile(location)
	if e != nil {
		return e
	}
	return compiled.Validate(value)
}
func Decode(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}
