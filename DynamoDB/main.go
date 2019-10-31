package DynamoDB

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// package to wrap DynamoDB functions
// making them cross implementation compatible etc

func GetDynamoClient(implementation string) (*dynamodb.DynamoDB, error) {

	if implementation == "lambda" {
		return dynamodb.New(session.New(), aws.NewConfig().WithRegion("ap-southeast-2")), nil
	} else {
		return nil, errors.New("Unknown implementation")
	}
}

func GetItem(key string, value interface{}, marshalTo interface{}, table string, implementation string) (interface{}, bool, error) {
	var input *dynamodb.GetItemInput

	switch t := value.(type) {
	case string:
		input = &dynamodb.GetItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				key: {
					S: aws.String(t),
				},
			},
		}
	default:
		return nil, false, errors.New("Unknown value type")
	}

	db, dbErr := GetDynamoClient(implementation)

	if dbErr != nil {
		return nil, false, dbErr
	}

	result, getError := db.GetItem(input)

	if getError != nil {
		return nil, false, getError
	}

	if result == nil || len(result.Item) == 0 {
		return nil, false, nil
	}

	marshalErr := dynamodbattribute.UnmarshalMap(result.Item, marshalTo)

	if marshalErr != nil {
		return nil, false, marshalErr
	}

	return marshalTo, true, nil
}

func GetItemBySecondaryIndex(key string, value interface{}, index string, marshalTo interface{}, table string, implementation string) (interface{}, error) {
	keyCondition := expression.Key(key).Equal(expression.Value(value))
	expr, errExpression := expression.NewBuilder().WithKeyCondition(keyCondition).Build()

	if errExpression != nil {
		return nil, errExpression
	}

	params := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(table),
		IndexName:                 aws.String(index),
		KeyConditionExpression:    expr.KeyCondition(),
	}
	db, dbErr := GetDynamoClient(implementation)

	if dbErr != nil {
		return nil, dbErr
	}

	result, getError := db.Query(params)

	if getError != nil {
		return nil, getError
	}

	if result == nil {
		return nil, nil
	}

	marshalErr := dynamodbattribute.UnmarshalListOfMaps(result.Items, marshalTo)

	if marshalErr != nil {
		return nil, marshalErr
	}
	return marshalTo, nil
}

func InsertItem(value interface{}, table string, implementation string) (*dynamodb.PutItemOutput, error) {
	toInsert, marshalErr := dynamodbattribute.MarshalMap(value)
	fmt.Println(toInsert)
	if marshalErr != nil {
		return nil, marshalErr
	}

	db, dbErr := GetDynamoClient(implementation)
	if dbErr != nil {
		return nil, dbErr
	}

	return db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      toInsert,
	})
}

func DeleteItem(key string, value interface{}, table string, implementation string) (bool, error) {
	var input *dynamodb.DeleteItemInput

	switch t := value.(type) {
	case string:
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				key: {
					S: aws.String(t),
				},
			},
		}
	default:
		return false, errors.New("Unknown value type")
	}

	db, dbErr := GetDynamoClient(implementation)

	if dbErr != nil {
		return false, dbErr
	}

	_, delError := db.DeleteItem(input)

	if delError != nil {
		return false, delError
	}

	return true, nil
}
